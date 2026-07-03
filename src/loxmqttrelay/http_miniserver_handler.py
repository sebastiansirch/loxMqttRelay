import asyncio
import aiohttp
from typing import Any
from loxmqttrelay.config import global_config
from loxmqttrelay.logging_config import get_lazy_logger
from loxmqttrelay.loxwebsocket_compat import apply_patches as apply_loxwebsocket_patches
from loxmqttrelay.websocket_ack import WebSocketAckWaiter
from loxmqttrelay.topic_sequencer import TopicSequencer
from loxwebsocket.lox_ws_api import loxwebsocket

logger = get_lazy_logger(__name__)

# Must run before any websocket traffic is sent - see loxwebsocket_compat.py
# for exactly what this fixes and why.
apply_loxwebsocket_patches()

# Initialize global instances with default values


class HttpMiniserverHandler:

    ms_ip = global_config.miniserver.miniserver_ip
    ms_port = global_config.miniserver.miniserver_port
    ms_user = global_config.miniserver.miniserver_user
    ms_pass = global_config.miniserver.miniserver_pass
    enable_mock_miniserver=global_config.debug.enable_mock
    mock_ms_ip=global_config.debug.mock_ip
    connection_semaphore = asyncio.Semaphore(global_config.miniserver.miniserver_max_parallel_connections)  # Default to 5 parallel connections
    target_ip = mock_ms_ip if (mock_ms_ip and enable_mock_miniserver) else ms_ip
    # Construct WebSocket URL with proper port handling
    protocol = "https" if ms_port == 443 else "http"
    if ms_port not in [80, 443]:
        ws_base_url = f"{protocol}://{target_ip}:{ms_port}"
        http_base_url = f"http://{target_ip}:{ms_port}"
    else:
        ws_base_url = f"{protocol}://{target_ip}"
        http_base_url = f"http://{target_ip}"
    auth = aiohttp.BasicAuth(ms_user, ms_pass) if ms_user and ms_pass else None
    # Increase the timeout to 10 seconds
    timeout = aiohttp.ClientTimeout(total=10)


    """Handler for processing and sending data to Miniserver via HTTP."""
    def __init__(self):
        logger.info("MQTT Miniserver Handler created")
        self._ws_ack_waiter = WebSocketAckWaiter()
        # send_websocket_command() ultimately does one unlocked
        # self._ws.send_str() on the single shared websocket connection;
        # aiohttp does not guarantee that concurrent writes from different
        # tasks stay un-interleaved. This serializes the encrypt+send step
        # (the only part that touches shared mutable state - the connection
        # and the encryption handler's salt) across concurrent callers,
        # while still letting each caller await its own response
        # independently afterwards.
        self._ws_write_lock = asyncio.Lock()
        # Serializes sends per topic (HTTP or WebSocket) so a retried older
        # value can never land after a newer one that already went out - see
        # topic_sequencer.py.
        self._topic_sequencer = TopicSequencer()

    async def _ensure_websocket_connected(self, ws_client, timeout: float = 30.0) -> bool:
        """
        Make sure ws_client is connected without racing the library's own
        internal reconnect loop: on a dropped connection, loxwebsocket calls
        its reconnect() internally, which calls async_init() directly - NOT
        through the connect()/_connect_lock path connect() uses. Calling
        connect() ourselves while that's already running would kick off a
        second, concurrent handshake against the same instance. If a
        reconnect is already under way, we just wait for it instead.
        """
        if ws_client.state == "CONNECTED":
            return True
        if ws_client.state == "RECONNECTING":
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while ws_client.state == "RECONNECTING" and loop.time() < deadline:
                await asyncio.sleep(0.2)
            return ws_client.state == "CONNECTED"
        await ws_client.connect(user=self.ms_user, password=self.ms_pass, loxone_url=self.ws_base_url, receive_updates=False)
        return ws_client.state == "CONNECTED"

    async def send_to_minisever_via_websocket(
        self,
        topic: str,
        normalized_topic: str,
        value: Any
    ) -> None:
        """
        Send data to the Loxone Miniserver via a WebSocket connection.

        send_websocket_command() itself does not wait for or check the
        Miniserver's response - it just writes to the socket. This waits for
        the matching response (correlated by topic, see websocket_ack.py) and
        retries transient failures (timeout, non-200 Code, connection issues)
        with exponential backoff, mirroring send_to_miniserver_via_http()'s
        retry behavior for the HTTP path.
        """
        logger.debug(f"Using miniserver address: {self.target_ip} {'(mock)' if (self.mock_ms_ip and self.enable_mock_miniserver) else '(real)'}")

        ws_client = loxwebsocket
        self._ws_ack_waiter.register(ws_client)

        max_attempts = max(1, global_config.miniserver.miniserver_websocket_retry_attempts)
        backoff_seconds = global_config.miniserver.miniserver_websocket_retry_backoff_seconds
        ack_timeout = global_config.miniserver.miniserver_websocket_ack_timeout_seconds

        for attempt in range(1, max_attempts + 1):
            is_last_attempt = attempt == max_attempts
            try:
                if not await self._ensure_websocket_connected(ws_client):
                    raise ConnectionError(f"WebSocket not connected (state={ws_client.state})")

                ack_future = self._ws_ack_waiter.start_wait(normalized_topic)
                async with self._ws_write_lock:
                    await ws_client.send_websocket_command(normalized_topic, str(value))
                ack = await self._ws_ack_waiter.await_ack(normalized_topic, ack_future, ack_timeout)

                if ack is not None and ack.get("Code") == "200":
                    logger.debug(f"Sent {topic} (as {normalized_topic})={value} to Miniserver successfully via WebSocket.")
                    return
                if ack is None:
                    detail = f"timed out after {ack_timeout}s waiting for a response"
                else:
                    detail = f"Miniserver returned Code={ack.get('Code')}"
                if is_last_attempt:
                    logger.error(
                        f"Error sending {topic} (as {normalized_topic})={value} to Miniserver via "
                        f"WebSocket: {detail}, giving up after {attempt} attempt(s)"
                    )
                    return
                logger.warning(
                    f"Error sending {topic} (as {normalized_topic})={value} to Miniserver via "
                    f"WebSocket: {detail}, retrying ({attempt}/{max_attempts})"
                )
            except asyncio.CancelledError:
                logger.error(f"WebSocket send for {topic} (as {normalized_topic})={value} was cancelled")
                return
            except Exception as e:
                if is_last_attempt:
                    logger.error(
                        f"Error sending {topic} (as {normalized_topic})={value} to Miniserver via "
                        f"WebSocket: {str(e)}, giving up after {attempt} attempt(s)"
                    )
                    return
                logger.warning(
                    f"Error sending {topic} (as {normalized_topic})={value} to Miniserver via "
                    f"WebSocket, retrying ({attempt}/{max_attempts}): {str(e)}"
                )

            await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))

        return


    async def send_to_miniserver_via_http(
        self,
        topic: str,
        normalized_topic: str,
        value: Any
    ) -> None:
        """
        Send data to Miniserver with rate limiting.
        If mock_ms_ip is provided and enable_mock_miniserver is True, mock server will be used instead of ms_ip.
        Returns a dictionary with results for each topic.
        """
        # Use mock miniserver IP only if both provided and enabled
        logger.debug(f"Using miniserver address: {self.target_ip} {'(mock)' if (self.mock_ms_ip and self.enable_mock_miniserver) else '(real)'}")

        async with aiohttp.ClientSession(auth=self.auth, timeout=self.timeout) as session:
            # Ensure value is converted to string
            safe_value = str(value)
            # Use pre-built HTTP base URL
            url = f"{self.http_base_url}/dev/sps/io/{normalized_topic}/{safe_value}"
            logger.debug(f"Sending to {url}")
            
            try:
                # Use semaphore to limit concurrent connections
                async with self.connection_semaphore:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            logger.warning(f"Miniserver returned {resp.status} for topic {topic} (URL: {url})")
                        else:
                            logger.debug(f"Sent {topic}={value} to Miniserver successfully.")
                        return { 'code': resp.status }
            except asyncio.TimeoutError:
                error_msg = f" Error 408: Timeout while sending {topic} (as {normalized_topic})={value} to Miniserver (URL: {url}): request timed out after 10 seconds"
                logger.error(error_msg)
                return 
            except asyncio.CancelledError:
                error_msg = f"Error 499: Request for {topic} (as {normalized_topic})={value} was cancelled (URL: {url})"
                logger.error(error_msg)
                return 
            except OSError as e:
                error_msg = f"Error 503: Connection error sending {topic} (as {normalized_topic})={value} to Miniserver (URL: {url}): {str(e)}"
                logger.error(error_msg)
                return 
            except aiohttp.ClientError as e:
                error_msg = f"Error 500: Client error sending {topic} (as {normalized_topic})={value} to Miniserver (URL: {url}): {str(e)}"
                logger.error(error_msg)
                return 
            except Exception as e:
                error_msg = f"Error 500: Unexpected error sending {topic} (as {normalized_topic})={value} to Miniserver (URL: {url}): {str(e)}"
                logger.error(error_msg)
                return 
    
    async def send_to_miniserver(
        self,
        topic: str,
        normalized_topic: str,
        value: Any,
    ) -> None:
        """
        Process data and send it to Miniserver.

        Queues the send through the per-topic sequencer instead of sending
        immediately: without that, two rapid messages on the same topic race
        (nothing upstream of this call - gmqtt's dispatch, the Rust
        forwarder - serializes them), and a retried older value could
        physically land at the Miniserver after a newer one that already
        succeeded. See topic_sequencer.py.

        Args:
            data: The data to process and send
            mqtt_publish_callback: Callback for MQTT publishing (required for topic forwarding)

        Returns:
            None
        """
        logger.debug(f"Sending {topic} (as {normalized_topic})={value} to Miniserver")

        async def _send() -> None:
            # Send to Miniserver using WebSocket or HTTP based on config
            if global_config.miniserver.use_websocket:
                await self.send_to_minisever_via_websocket(topic, normalized_topic, value)
            else:
                await self.send_to_miniserver_via_http(topic, normalized_topic, value)

        await self._topic_sequencer.submit(
            normalized_topic,
            _send,
            coalesce=global_config.miniserver.miniserver_coalesce_topic_updates,
            description=f"{normalized_topic}={value}",
        )

http_miniserver_handler = HttpMiniserverHandler()