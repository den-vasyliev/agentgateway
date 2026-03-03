use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use ::http::StatusCode;
use ::http::header::CONTENT_TYPE;
use ::http::request::Parts;
use agent_core::version::BuildInfo;
use anyhow::anyhow;
use futures_util::StreamExt;
use headers::HeaderMapExt;
use rmcp::model::{
	ClientInfo, ClientJsonRpcMessage, ClientNotification, ClientRequest, ClientResult, ConstString,
	Implementation, ProtocolVersion, RequestId, ServerJsonRpcMessage,
};
use rmcp::transport::common::http_header::{EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE};
use serde_json::Map as JsonObject;
use sse_stream::{KeepAlive, Sse, SseBody, SseStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

use crate::http::Response;
use crate::mcp::handler::Relay;
use crate::mcp::mergestream::Messages;
use crate::mcp::streamablehttp::{ServerSseMessage, StreamableHttpPostResponse};
use crate::mcp::upstream::{IncomingRequestContext, UpstreamError};
use crate::mcp::{ClientError, MCPOperation, rbac};
use crate::proxy::ProxyError;
use crate::{mcp, *};

/// Shared slot for the server→client SSE sender, populated when a GET stream is open.
type SharedServerTx = Arc<Mutex<Option<Sender<ServerJsonRpcMessage>>>>;

/// Pending server→client round-trips: request ID → oneshot for the client's result.
type PendingMap = Arc<Mutex<HashMap<RequestId, oneshot::Sender<ClientResult>>>>;

#[derive(Debug, Clone)]
pub struct Session {
	encoder: http::sessionpersistence::Encoder,
	relay: Arc<Relay>,
	pub id: Arc<str>,
	/// Channel for legacy SSE sessions (deprecated transport).
	tx: Option<Sender<ServerJsonRpcMessage>>,
	/// Shared sender to forward server-initiated requests to the downstream GET SSE stream.
	/// All clones of a Session share the same Arc so that a POST handler can reach the
	/// sender that was registered by a concurrent GET handler.
	server_tx: SharedServerTx,
	/// Pending server→client round-trips keyed by request ID.
	/// Shared across all clones of the same session.
	pending: PendingMap,
}

impl Session {
	/// send a message to upstream server(s)
	pub async fn send(
		&mut self,
		parts: Parts,
		message: ClientJsonRpcMessage,
	) -> Result<Response, ProxyError> {
		let req_id = match &message {
			ClientJsonRpcMessage::Request(r) => Some(r.id.clone()),
			_ => None,
		};
		Self::handle_error(req_id, self.send_internal(parts, message).await).await
	}

	/// send a message to upstream server(s), when using stateless mode. In stateless mode, every
	/// message is wrapped in an InitializeRequest (except the actual InitializeRequest from the
	/// downstream). This ensures servers that require an InitializeRequest behave correctly.
	pub async fn stateless_send_and_initialize(
		&mut self,
		parts: Parts,
		message: ClientJsonRpcMessage,
	) -> Result<Response, ProxyError> {
		let is_init = matches!(&message, ClientJsonRpcMessage::Request(r) if matches!(&r.request, &ClientRequest::InitializeRequest(_)));
		if !is_init {
			// first, send the initialize
			let init_request = rmcp::model::InitializeRequest {
				method: Default::default(),
				params: get_client_info(),
				extensions: Default::default(),
			};
			let _ = self
				.send(
					parts.clone(),
					ClientJsonRpcMessage::request(init_request.into(), RequestId::Number(0)),
				)
				.await?;

			// And we need to notify as well.
			let notification = ClientJsonRpcMessage::notification(
				rmcp::model::InitializedNotification {
					method: Default::default(),
					extensions: Default::default(),
				}
				.into(),
			);
			let _ = self.send(parts.clone(), notification).await?;
		}
		// Now we can send the message like normal
		self.send(parts, message).await
	}

	/// delete any active sessions
	pub async fn delete_session(&self, parts: Parts) -> Result<Response, ProxyError> {
		let ctx = IncomingRequestContext::new(&parts);
		let (_span, log, _cel) = mcp::handler::setup_request_log(parts, "delete_session");
		let session_id = self.id.to_string();
		log.non_atomic_mutate(|l| {
			// NOTE: l.method_name keep None to respect the metrics logic: not handle GET, DELETE.
			l.session_id = Some(session_id);
		});
		Self::handle_error(None, self.relay.send_fanout_deletion(ctx).await).await
	}

	/// forward_legacy_sse takes an upstream Response and forwards all messages to the SSE data stream.
	/// In SSE, POST requests always just get a 202 response and the messages go on a separate stream.
	/// Note: its plausible we could rewrite the rest of the proxy to return a more structured type than
	/// `Response` here, so we don't have to re-process it. However, since SSE is deprecated its best to
	/// optimize for the non-deprecated code paths; this works fine.
	pub async fn forward_legacy_sse(&self, resp: Response) -> Result<(), ClientError> {
		let Some(tx) = self.tx.clone() else {
			return Err(ClientError::new(anyhow!(
				"may only be called for SSE streams",
			)));
		};
		let content_type = resp.headers().get(CONTENT_TYPE);
		let sse = match content_type {
			Some(ct) if ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) => {
				trace!("forward SSE got SSE stream response");
				let content_encoding = resp.headers().typed_get::<headers::ContentEncoding>();
				let (body, _encoding) =
					crate::http::compression::decompress_body(resp.into_body(), content_encoding.as_ref())
						.map_err(ClientError::new)?;
				let event_stream = SseStream::from_byte_stream(body.into_data_stream()).boxed();
				StreamableHttpPostResponse::Sse(event_stream, None)
			},
			Some(ct) if ct.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) => {
				trace!("forward SSE got single JSON response");
				let message = json::from_response_body::<ServerJsonRpcMessage>(resp)
					.await
					.map_err(ClientError::new)?;
				StreamableHttpPostResponse::Json(message, None)
			},
			_ => {
				trace!("forward SSE got accepted, no action needed");
				return Ok(());
			},
		};
		let mut ms: Messages = sse.try_into()?;
		tokio::spawn(async move {
			while let Some(Ok(msg)) = ms.next().await {
				let Ok(()) = tx.send(msg).await else {
					return;
				};
			}
		});
		Ok(())
	}

	/// Deliver a sampling/createMessage response from the downstream client.
	/// Called by handle_post when it receives a ClientJsonRpcMessage::Response.
	/// Returns true if the ID matched a pending sampling round-trip, false otherwise.
	pub fn deliver_sampling_response(&self, id: &RequestId, result: ClientResult) -> bool {
		let mut pending = self.pending.lock().expect("pending lock");
		if let Some(tx) = pending.remove(id) {
			let _ = tx.send(result);
			true
		} else {
			false
		}
	}

	/// get_stream establishes the downstream GET SSE stream.
	///
	/// It merges two sources:
	/// 1. Upstream GET SSE (notifications from upstream servers)
	/// 2. Server-initiated requests forwarded from concurrent POST handlers (sampling)
	///
	/// The merged stream is returned as an SSE response to the downstream client.
	pub async fn get_stream(&self, parts: Parts) -> Result<Response, ProxyError> {
		let ctx = IncomingRequestContext::new(&parts);
		let (_span, log, _cel) = mcp::handler::setup_request_log(parts, "get_stream");
		let session_id = self.id.to_string();
		log.non_atomic_mutate(|l| {
			l.session_id = Some(session_id);
		});

		// Register a sender so concurrent send_internal calls can forward server→client requests.
		let (server_tx, server_rx) = tokio::sync::mpsc::channel::<ServerJsonRpcMessage>(64);
		{
			let mut slot = self.server_tx.lock().expect("server_tx lock");
			*slot = Some(server_tx);
		}

		// Build the upstream GET SSE stream.
		let upstream_resp = self.relay.send_fanout_get(ctx).await;
		let upstream_resp = match Self::handle_error(None, upstream_resp).await {
			Ok(r) => r,
			Err(e) => {
				// Clear the server_tx slot on error so we don't leak the channel.
				self.server_tx.lock().expect("server_tx lock").take();
				return Err(e);
			},
		};

		// Convert the upstream HTTP response body into a stream of ServerSseMessage.
		// The upstream response is already an SSE stream from relay.send_fanout_get.
		// We need to wrap it together with server_rx.
		//
		// relay.send_fanout_get returns a Response whose body is an SSE stream of ServerSseMessage.
		// We cannot easily re-stream it as ServerSseMessage items without re-parsing. Instead, we
		// forward server_rx items as ServerSseMessage into the same response body via a tokio task
		// that writes into a channel that feeds the SSE body.
		//
		// Simplest approach: use a merged mpsc channel. We spawn a task that reads from the
		// upstream response body and forwards each SSE event, while also reading from server_rx.
		// Both are forwarded to a single merged_tx -> merged_rx that feeds the SSE response.

		let (merged_tx, merged_rx) = tokio::sync::mpsc::channel::<ServerSseMessage>(128);

		// Spawn task: forward upstream SSE → merged channel.
		{
			let merged_tx = merged_tx.clone();
			// Parse the upstream response body back into ServerJsonRpcMessage items.
			let content_encoding = upstream_resp
				.headers()
				.typed_get::<headers::ContentEncoding>();
			let body = match crate::http::compression::decompress_body(
				upstream_resp.into_body(),
				content_encoding.as_ref(),
			) {
				Ok((b, _)) => b,
				Err(_) => {
					self.server_tx.lock().expect("server_tx lock").take();
					return Err(
						mcp::Error::SendError(None, "failed to decompress upstream GET body".to_string())
							.into(),
					);
				},
			};
			let mut sse_stream = SseStream::from_byte_stream(body.into_data_stream());
			tokio::spawn(async move {
				while let Some(item) = sse_stream.next().await {
					let event = match item {
						Ok(e) => e,
						Err(_) => break,
					};
					let data = match event.data {
						Some(d) if !d.is_empty() => d,
						_ => continue,
					};
					let msg = match serde_json::from_str::<ServerJsonRpcMessage>(&data) {
						Ok(m) => m,
						Err(_) => continue,
					};
					let sse_msg = ServerSseMessage {
						event_id: event.id,
						message: Arc::new(msg),
					};
					if merged_tx.send(sse_msg).await.is_err() {
						break;
					}
				}
			});
		}

		// Spawn task: forward server_rx (sampling requests) → merged channel.
		{
			let merged_tx = merged_tx.clone();
			let mut server_rx = server_rx;
			tokio::spawn(async move {
				while let Some(msg) = server_rx.recv().await {
					let sse_msg = ServerSseMessage {
						event_id: None,
						message: Arc::new(msg),
					};
					if merged_tx.send(sse_msg).await.is_err() {
						break;
					}
				}
			});
		}

		// Return the merged SSE stream as the GET response.
		let stream = tokio_stream::wrappers::ReceiverStream::new(merged_rx);
		Ok(sse_stream_response(stream, None))
	}

	async fn handle_error(
		req_id: Option<RequestId>,
		d: Result<Response, UpstreamError>,
	) -> Result<Response, ProxyError> {
		match d {
			Ok(r) => Ok(r),
			Err(UpstreamError::Http(ClientError::Status(resp))) => {
				let resp = http::SendDirectResponse::new(*resp)
					.await
					.map_err(ProxyError::Body)?;
				Err(mcp::Error::UpstreamError(Box::new(resp)).into())
			},
			Err(UpstreamError::Proxy(p)) => Err(p),
			Err(UpstreamError::Authorization {
				resource_type,
				resource_name,
			}) if req_id.is_some() => {
				Err(mcp::Error::Authorization(req_id.unwrap(), resource_type, resource_name).into())
			},
			// TODO: this is too broad. We have a big tangle of errors to untangle though
			Err(e) => Err(mcp::Error::SendError(req_id, e.to_string()).into()),
		}
	}

	async fn send_internal(
		&mut self,
		parts: Parts,
		message: ClientJsonRpcMessage,
	) -> Result<Response, UpstreamError> {
		// Sending a message entails fanning out the message to each upstream, and then aggregating the responses.
		// The responses may include any number of notifications on the same HTTP response, and then finish with the
		// response to the request.
		// To merge these, we use a MergeStream which will join all of the notifications together, and then apply
		// some per-request merge logic across all the responses.
		// For example, this may return [server1-notification, server2-notification, server2-notification, merge(server1-response, server2-response)].
		// It's very common to not have any notifications, though.
		match message {
			ClientJsonRpcMessage::Request(mut r) => {
				let method = r.request.method();
				let ctx = IncomingRequestContext::new(&parts);
				let (_span, log, cel) = mcp::handler::setup_request_log(parts, method);
				let session_id = self.id.to_string();
				log.non_atomic_mutate(|l| {
					l.method_name = Some(method.to_string());
					l.session_id = Some(session_id);
				});
				match &mut r.request {
					ClientRequest::InitializeRequest(ir) => {
						if self.relay.is_multiplexing() {
							// Currently, we cannot support roots until we have a mapping of downstream and upstream ID.
							// However, the clients can tell the server they support roots.
							// Instead, we hijack this to tell them not to so they do not send requests that we cannot
							// actually support
							ir.params.capabilities.roots = None
						}
						let pv = ir.params.protocol_version.clone();
						let res = self
							.relay
							.send_fanout(
								r,
								ctx,
								self
									.relay
									.merge_initialize(pv, self.relay.is_multiplexing()),
							)
							.await;
						if let Some(sessions) = self.relay.get_sessions() {
							let s = http::sessionpersistence::SessionState::MCP(
								http::sessionpersistence::MCPSessionState { sessions },
							);
							if let Ok(id) = s.encode(&self.encoder) {
								self.id = id.into();
							}
						}
						res
					},
					ClientRequest::ListToolsRequest(_) => {
						log.non_atomic_mutate(|l| {
							l.resource = Some(MCPOperation::Tool);
						});
						self
							.relay
							.send_fanout(r, ctx, self.relay.merge_tools(cel))
							.await
					},
					ClientRequest::PingRequest(_) | ClientRequest::SetLevelRequest(_) => {
						self
							.relay
							.send_fanout(r, ctx, self.relay.merge_empty())
							.await
					},
					ClientRequest::ListPromptsRequest(_) => {
						log.non_atomic_mutate(|l| {
							l.resource = Some(MCPOperation::Prompt);
						});
						self
							.relay
							.send_fanout(r, ctx, self.relay.merge_prompts(cel))
							.await
					},
					ClientRequest::ListResourcesRequest(_) => {
						if !self.relay.is_multiplexing() {
							log.non_atomic_mutate(|l| {
								l.resource = Some(MCPOperation::Resource);
							});
							self
								.relay
								.send_fanout(r, ctx, self.relay.merge_resources(cel))
								.await
						} else {
							// TODO(https://github.com/agentgateway/agentgateway/issues/404)
							// Find a mapping of URL
							Err(UpstreamError::InvalidMethodWithMultiplexing(
								r.request.method().to_string(),
							))
						}
					},
					ClientRequest::ListResourceTemplatesRequest(_) => {
						if !self.relay.is_multiplexing() {
							log.non_atomic_mutate(|l| {
								l.resource = Some(MCPOperation::ResourceTemplates);
							});
							self
								.relay
								.send_fanout(r, ctx, self.relay.merge_resource_templates(cel))
								.await
						} else {
							// TODO(https://github.com/agentgateway/agentgateway/issues/404)
							// Find a mapping of URL
							Err(UpstreamError::InvalidMethodWithMultiplexing(
								r.request.method().to_string(),
							))
						}
					},
					ClientRequest::CallToolRequest(ctr) => {
						let name = ctr.params.name.clone();
						let (service_name, tool) = self.relay.parse_resource_name(&name)?;
						log.non_atomic_mutate(|l| {
							l.resource_name = Some(tool.to_string());
							l.target_name = Some(service_name.to_string());
							l.resource = Some(MCPOperation::Tool);
						});
						if !self.relay.policies.validate(
							&rbac::ResourceType::Tool(rbac::ResourceId::new(
								service_name.to_string(),
								tool.to_string(),
							)),
							&cel,
						) {
							return Err(UpstreamError::Authorization {
								resource_type: "tool".to_string(),
								resource_name: name.to_string(),
							});
						}

						let tn = tool.to_string();
						ctr.params.name = tn.into();

						// Wrap the upstream response to relay any server→client requests
						// (e.g. sampling/createMessage) that arrive inline on the POST SSE stream.
						let upstream_result = self.relay.send_single(r, ctx, service_name).await?;
						self
							.relay_server_requests_from_response(upstream_result, &self.server_tx, &self.pending)
							.await
					},
					ClientRequest::GetPromptRequest(gpr) => {
						let name = gpr.params.name.clone();
						let (service_name, prompt) = self.relay.parse_resource_name(&name)?;
						log.non_atomic_mutate(|l| {
							l.target_name = Some(service_name.to_string());
							l.resource_name = Some(prompt.to_string());
							l.resource = Some(MCPOperation::Prompt);
						});
						if !self.relay.policies.validate(
							&rbac::ResourceType::Prompt(rbac::ResourceId::new(
								service_name.to_string(),
								prompt.to_string(),
							)),
							&cel,
						) {
							return Err(UpstreamError::Authorization {
								resource_type: "prompt".to_string(),
								resource_name: name.to_string(),
							});
						}
						gpr.params.name = prompt.to_string();
						self.relay.send_single(r, ctx, service_name).await
					},
					ClientRequest::ReadResourceRequest(rrr) => {
						if let Some(service_name) = self.relay.default_target_name() {
							let uri = rrr.params.uri.clone();
							log.non_atomic_mutate(|l| {
								l.target_name = Some(service_name.to_string());
								l.resource_name = Some(uri.to_string());
								l.resource = Some(MCPOperation::Resource);
							});
							if !self.relay.policies.validate(
								&rbac::ResourceType::Resource(rbac::ResourceId::new(
									service_name.to_string(),
									uri.to_string(),
								)),
								&cel,
							) {
								return Err(UpstreamError::Authorization {
									resource_type: "resource".to_string(),
									resource_name: uri.to_string(),
								});
							}
							self.relay.send_single_without_multiplexing(r, ctx).await
						} else {
							// TODO(https://github.com/agentgateway/agentgateway/issues/404)
							// Find a mapping of URL
							Err(UpstreamError::InvalidMethodWithMultiplexing(
								r.request.method().to_string(),
							))
						}
					},

					ClientRequest::ListTasksRequest(_)
					| ClientRequest::GetTaskInfoRequest(_)
					| ClientRequest::GetTaskResultRequest(_)
					| ClientRequest::CancelTaskRequest(_)
					| ClientRequest::SubscribeRequest(_)
					| ClientRequest::UnsubscribeRequest(_)
					| ClientRequest::CustomRequest(_) => {
						// TODO(https://github.com/agentgateway/agentgateway/issues/404)
						Err(UpstreamError::InvalidMethod(r.request.method().to_string()))
					},
					ClientRequest::CompleteRequest(_) => {
						// For now, we don't have a sane mapping of incoming requests to a specific
						// downstream service when multiplexing. Only forward when we have only one backend.
						self.relay.send_single_without_multiplexing(r, ctx).await
					},
				}
			},
			ClientJsonRpcMessage::Notification(r) => {
				let method = match &r.notification {
					ClientNotification::CancelledNotification(r) => r.method.as_str(),
					ClientNotification::ProgressNotification(r) => r.method.as_str(),
					ClientNotification::InitializedNotification(r) => r.method.as_str(),
					ClientNotification::RootsListChangedNotification(r) => r.method.as_str(),
					ClientNotification::CustomNotification(r) => r.method.as_str(),
				};
				let ctx = IncomingRequestContext::new(&parts);
				let (_span, log, _cel) = mcp::handler::setup_request_log(parts, method);
				let session_id = self.id.to_string();
				log.non_atomic_mutate(|l| {
					l.method_name = Some(method.to_string());
					l.session_id = Some(session_id);
				});
				// TODO: the notification needs to be fanned out in some cases and sent to a single one in others
				// however, we don't have a way to map to the correct service yet
				self.relay.send_notification(r, ctx).await
			},

			_ => Err(UpstreamError::InvalidRequest(
				"unsupported message type".to_string(),
			)),
		}
	}

	/// Scan an upstream POST response for server→client Request messages (e.g. sampling/createMessage).
	/// For each such request: forward it to the downstream GET SSE stream via server_tx, register a
	/// oneshot in pending, and await the client's response. The client response is injected back into
	/// the upstream stream via a spawned task that intercepts it before it reaches the merge logic.
	///
	/// Non-request messages (Notifications, Responses) are passed through unchanged.
	///
	/// This is called only for tool calls, which are the only requests that can trigger sampling.
	async fn relay_server_requests_from_response(
		&self,
		upstream_resp: Response,
		server_tx: &SharedServerTx,
		pending: &PendingMap,
	) -> Result<Response, UpstreamError> {
		// If the response is not SSE (i.e. it's a plain JSON response), there are no inline
		// server→client requests to handle — return as-is.
		let content_type = upstream_resp.headers().get(CONTENT_TYPE).cloned();
		let is_sse = content_type
			.as_ref()
			.is_some_and(|ct| ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()));
		if !is_sse {
			return Ok(upstream_resp);
		}

		// Parse the upstream SSE stream and intercept server→client Request items.
		// We buffer them through a new channel and return that as the modified SSE response.
		let (out_tx, out_rx) = tokio::sync::mpsc::channel::<ServerJsonRpcMessage>(128);
		let server_tx = server_tx.clone();
		let pending = pending.clone();

		let content_encoding = upstream_resp
			.headers()
			.typed_get::<headers::ContentEncoding>();
		let body = match crate::http::compression::decompress_body(
			upstream_resp.into_body(),
			content_encoding.as_ref(),
		) {
			Ok((b, _)) => b,
			Err(e) => return Err(UpstreamError::Http(ClientError::new(e))),
		};

		tokio::spawn(async move {
			let mut sse_stream = SseStream::from_byte_stream(body.into_data_stream());
			while let Some(item) = sse_stream.next().await {
				let event = match item {
					Ok(e) => e,
					Err(_) => break,
				};
				let data = match event.data {
					Some(d) if !d.is_empty() => d,
					_ => continue,
				};
				let msg = match serde_json::from_str::<ServerJsonRpcMessage>(&data) {
					Ok(m) => m,
					Err(_) => continue,
				};

				// Check if this is a server→client Request (e.g. sampling/createMessage).
				if let ServerJsonRpcMessage::Request(ref req) = msg {
					let req_id = req.id.clone();

					// Forward the request to the downstream GET SSE stream.
					// Clone the Sender out of the mutex before awaiting to avoid holding
					// a MutexGuard across an await point (which is not Send).
					let maybe_tx = server_tx.lock().expect("server_tx lock").clone();
					let forwarded = if let Some(tx) = maybe_tx {
						tx.send(msg.clone()).await.is_ok()
					} else {
						false
					};

					if forwarded {
						// Register oneshot to receive the client's response.
						let (resp_tx, resp_rx) = oneshot::channel::<ClientResult>();
						pending
							.lock()
							.expect("pending lock")
							.insert(req_id.clone(), resp_tx);

						// Wait for the client to POST the response (up to 5 minutes).
						match tokio::time::timeout(std::time::Duration::from_secs(300), resp_rx).await {
							Ok(Ok(result)) => {
								// Synthesize a response message and send it upstream (as the
								// continuation of this SSE stream — the upstream server is reading
								// events from us via the POST body stream, but for StreamableHTTP
								// the client response comes back on a separate POST).
								// We don't need to write the result back to the upstream SSE here;
								// the upstream server already sent us the tool result after receiving
								// the sampling response via the client's dedicated sampling POST.
								// Just drop it — the upstream will send the final tool result on the
								// SSE stream momentarily.
								debug!("sampling response received for id={:?}", req_id);
								let _ = result; // consumed
							},
							Ok(Err(_)) => {
								debug!("sampling oneshot cancelled for id={:?}", req_id);
							},
							Err(_) => {
								debug!("sampling response timeout for id={:?}", req_id);
								pending.lock().expect("pending lock").remove(&req_id);
							},
						}
						// Do not forward the server Request to the output stream —
						// it was already sent to the GET SSE stream above.
						continue;
					}
					// No GET stream open; fall through and forward as-is (client can't handle it).
					warn!(
						"sampling/createMessage received but no GET stream is open — forwarding as notification"
					);
				}

				// Pass through Responses, Notifications, and unhandled Requests.
				if out_tx.send(msg).await.is_err() {
					break;
				}
			}
		});

		// Reconstruct an SSE Response from the out_rx channel.
		let stream = tokio_stream::wrappers::ReceiverStream::new(out_rx).map(|msg| ServerSseMessage {
			event_id: None,
			message: Arc::new(msg),
		});
		Ok(sse_stream_response(stream, None))
	}
}

#[derive(Debug)]
pub struct SessionManager {
	encoder: http::sessionpersistence::Encoder,
	sessions: RwLock<HashMap<String, Session>>,
}

fn session_id() -> Arc<str> {
	uuid::Uuid::new_v4().to_string().into()
}

impl SessionManager {
	pub fn new(encoder: http::sessionpersistence::Encoder) -> Self {
		Self {
			encoder,
			sessions: Default::default(),
		}
	}

	pub fn get_session(&self, id: &str) -> Option<Session> {
		self.sessions.read().ok()?.get(id).cloned()
	}

	pub fn get_or_resume_session(
		&self,
		id: &str,
		builder: Arc<dyn Fn() -> Result<Relay, http::Error> + Send + Sync>,
	) -> Option<Session> {
		if let Some(s) = self.sessions.read().ok()?.get(id).cloned() {
			return Some(s);
		}
		let d = http::sessionpersistence::SessionState::decode(id, &self.encoder).ok()?;
		let http::sessionpersistence::SessionState::MCP(state) = d else {
			return None;
		};
		let relay = builder().ok()?;
		let n = relay.count();
		if state.sessions.len() != n {
			warn!(
				"failed to resume session: sessions {} did not match config {}",
				state.sessions.len(),
				n
			);
			return None;
		}
		relay.set_sessions(state.sessions);

		let sess = Session {
			id: id.into(),
			relay: Arc::new(relay),
			tx: None,
			server_tx: Arc::new(Mutex::new(None)),
			pending: Arc::new(Mutex::new(HashMap::new())),
			encoder: self.encoder.clone(),
		};
		let mut sm = self.sessions.write().expect("write lock");
		sm.insert(id.to_string(), sess.clone());
		Some(sess)
	}

	/// create_session establishes an MCP session.
	pub fn create_session(&self, relay: Relay) -> Session {
		let id = session_id();

		// Do NOT insert yet
		Session {
			id: id.clone(),
			relay: Arc::new(relay),
			tx: None,
			server_tx: Arc::new(Mutex::new(None)),
			pending: Arc::new(Mutex::new(HashMap::new())),
			encoder: self.encoder.clone(),
		}
	}

	pub fn insert_session(&self, sess: Session) {
		let mut sm = self.sessions.write().expect("write lock");
		sm.insert(sess.id.to_string(), sess);
	}

	/// create_stateless_session creates a session for stateless mode.
	/// Unlike create_session, this does NOT register the session in the session manager.
	/// The caller is responsible for calling session.delete_session() when done
	/// to clean up upstream resources (e.g., stdio processes).
	pub fn create_stateless_session(&self, relay: Relay) -> Session {
		let id = session_id();
		Session {
			id,
			relay: Arc::new(relay),
			tx: None,
			server_tx: Arc::new(Mutex::new(None)),
			pending: Arc::new(Mutex::new(HashMap::new())),
			encoder: self.encoder.clone(),
		}
	}

	/// create_legacy_session establishes a legacy SSE session.
	/// These will have the ability to send messages to them via a channel.
	pub fn create_legacy_session(&self, relay: Relay) -> (Session, Receiver<ServerJsonRpcMessage>) {
		let (tx, rx) = tokio::sync::mpsc::channel(64);
		let id = session_id();
		let sess = Session {
			id: id.clone(),
			relay: Arc::new(relay),
			tx: Some(tx),
			server_tx: Arc::new(Mutex::new(None)),
			pending: Arc::new(Mutex::new(HashMap::new())),
			encoder: self.encoder.clone(),
		};
		let mut sm = self.sessions.write().expect("write lock");
		sm.insert(id.to_string(), sess.clone());
		(sess, rx)
	}

	pub async fn delete_session(&self, id: &str, parts: Parts) -> Option<Response> {
		let sess = {
			let mut sm = self.sessions.write().expect("write lock");
			sm.remove(id)?
		};
		// Swallow the error
		sess.delete_session(parts).await.ok()
	}
}

#[derive(Debug, Clone)]
pub struct SessionDropper {
	sm: Arc<SessionManager>,
	s: Option<(Session, Parts)>,
}

/// Dropper returns a handle that, when dropped, removes the session
pub fn dropper(sm: Arc<SessionManager>, s: Session, parts: Parts) -> SessionDropper {
	SessionDropper {
		sm,
		s: Some((s, parts)),
	}
}

impl Drop for SessionDropper {
	fn drop(&mut self) {
		let Some((s, parts)) = self.s.take() else {
			return;
		};
		let mut sm = self.sm.sessions.write().expect("write lock");
		debug!("delete session {}", s.id);
		sm.remove(s.id.as_ref());
		tokio::task::spawn(async move { s.delete_session(parts).await });
	}
}

pub(crate) fn sse_stream_response(
	stream: impl futures::Stream<Item = ServerSseMessage> + Send + 'static,
	keep_alive: Option<Duration>,
) -> Response {
	use futures::StreamExt;
	let stream = SseBody::new(stream.map(|message| {
		let data = serde_json::to_string(&message.message).expect("valid message");
		let mut sse = Sse::default().data(data);
		sse.id = message.event_id;
		Result::<Sse, Infallible>::Ok(sse)
	}));
	let stream = match keep_alive {
		Some(duration) => {
			http::Body::new(stream.with_keep_alive::<TokioSseTimer>(KeepAlive::new().interval(duration)))
		},
		None => http::Body::new(stream),
	};
	::http::Response::builder()
		.status(StatusCode::OK)
		.header(http::header::CONTENT_TYPE, EVENT_STREAM_MIME_TYPE)
		.header(http::header::CACHE_CONTROL, "no-cache")
		.body(stream)
		.expect("valid response")
}

pin_project_lite::pin_project! {
		struct TokioSseTimer {
				#[pin]
				sleep: tokio::time::Sleep,
		}
}
impl Future for TokioSseTimer {
	type Output = ();

	fn poll(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Self::Output> {
		let this = self.project();
		this.sleep.poll(cx)
	}
}
impl sse_stream::Timer for TokioSseTimer {
	fn from_duration(duration: Duration) -> Self {
		Self {
			sleep: tokio::time::sleep(duration),
		}
	}

	fn reset(self: std::pin::Pin<&mut Self>, when: std::time::Instant) {
		let this = self.project();
		this.sleep.reset(tokio::time::Instant::from_std(when));
	}
}

fn get_client_info() -> ClientInfo {
	ClientInfo {
		meta: None,
		protocol_version: ProtocolVersion::V_2025_06_18,
		capabilities: rmcp::model::ClientCapabilities {
			experimental: None,
			roots: None,
			// Advertise sampling support so upstream servers can invoke sampling/createMessage.
			sampling: Some(JsonObject::new()),
			elicitation: None,
			tasks: None,
		},
		client_info: Implementation {
			name: "agentgateway".to_string(),
			version: BuildInfo::new().version.to_string(),
			..Default::default()
		},
	}
}
