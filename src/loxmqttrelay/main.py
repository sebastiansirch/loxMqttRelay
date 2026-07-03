import asyncio
import types
import sys
import os
import orjson
import uvloop

from loxmqttrelay.config import ConfigError, ConfigSection, global_config
from loxmqttrelay.logging_config import get_lazy_logger
from loxmqttrelay.mqtt_client import mqtt_client
from loxmqttrelay.udp_handler import start_udp_server
from loxmqttrelay.miniserver_sync import sync_miniserver_whitelist
from loxmqttrelay.http_miniserver_handler import http_miniserver_handler
import loxmqttrelay.utils as utils

# The imports are now handled by __init__.py
from loxmqttrelay import MiniserverDataProcessor, init_rust_logger

TOPIC = types.SimpleNamespace(
    CONFIG_SET = f"{global_config.general.base_topic}config/set",
    CONFIG_ADD = f"{global_config.general.base_topic}config/add",
    CONFIG_REMOVE = f"{global_config.general.base_topic}config/remove",
    CONFIG_UPDATE = f"{global_config.general.base_topic}config/update",
    CONFIG_RESTART = f"{global_config.general.base_topic}config/restart",
    CONFIG_GET = f"{global_config.general.base_topic}config/get",
    CONFIG_RESPONSE = f"{global_config.general.base_topic}config/response",
    MINISERVER_STARTUP_EVENT = f"{global_config.general.base_topic}miniserverevent/startup"
)

logger = get_lazy_logger(__name__)

# Initialize Rust logger (native call — log a breadcrumb so a hard crash here
# is preceded by a traceable log line).
logger.info("Initializing Rust logger ...")
init_rust_logger()

class MQTTRelay:
    def __init__(self):
        self.miniserver_data_processor = MiniserverDataProcessor(TOPIC, global_config, self, mqtt_client, http_miniserver_handler, orjson)

    async def main(self):
        await self.connect_and_subscribe_mqtt()
        await self.handle_miniserver_sync()
        asyncio.create_task(start_udp_server())
        asyncio.create_task(self.periodic_miniserver_sync())

        logger.info("MQTT Relay started")
        await asyncio.Future()

    async def handle_miniserver_sync(self):
        """Attempt to sync whitelist with miniserver if enabled"""
        if not global_config.miniserver.sync_with_miniserver:
            return

        # Store initial whitelist from config
        initial_whitelist = global_config.topics.topic_whitelist.copy()
        defensive = global_config.miniserver.whitelist_sync_defensive

        try:
            inputs = await sync_miniserver_whitelist()
            # Defensive mode only ADDS topics discovered by this sync instead of
            # replacing the whitelist outright, so a sync that (transiently, or
            # due to a Miniserver-side race - see docs/) returns an incomplete
            # list can never silently drop a previously-known-good topic.
            list_mode = "add" if defensive else "set"
            global_config.update_config(ConfigSection.TOPICS, {'topic_whitelist': inputs}, list_mode=list_mode)
            merged_whitelist = list(global_config.topics.topic_whitelist)
            self.miniserver_data_processor.update_topic_whitelist(merged_whitelist)
            logger.info(
                f"Whitelist updated from miniserver configuration "
                f"(mode={list_mode}, {len(inputs)} synced, {len(merged_whitelist)} total)"
            )
        except Exception as e:
            logger.error(f"Failed to sync with miniserver: {str(e)}")
            logger.info("Keeping whitelist from config")
            global_config.update_config(ConfigSection.TOPICS, {'topic_whitelist': initial_whitelist})
            self.miniserver_data_processor.update_topic_whitelist(list(initial_whitelist))

    async def periodic_miniserver_sync(self):
        """
        Re-run the miniserver whitelist sync on a fixed interval, if configured.
        This means a Miniserver reboot that fails to publish
        "miniserverevent/startup" (or whose publish is missed) no longer
        requires a relay restart to pick up whitelist changes.
        """
        interval = global_config.miniserver.whitelist_sync_interval_seconds
        if interval <= 0:
            return

        if not global_config.miniserver.whitelist_sync_defensive:
            logger.warning(
                "Periodic miniserver sync is enabled with whitelist_sync_defensive=False: "
                "each periodic sync fully replaces the whitelist, so a transiently incomplete "
                "sync can silently drop topics again on the next interval. Consider enabling "
                "whitelist_sync_defensive."
            )

        logger.info(f"Periodic miniserver whitelist sync enabled every {interval}s")
        while True:
            await asyncio.sleep(interval)
            logger.info("Periodic miniserver sync triggered")
            await self.handle_miniserver_sync()

    # UPDATED: Synchronous wrapper with added logging to help testing
    def schedule_miniserver_sync(self):
        """Schedule the asynchronous handle_miniserver_sync in the event loop."""
        logger.info("Miniserver startup detected, resyncing whitelist")
        asyncio.create_task(self.handle_miniserver_sync())

    async def connect_and_subscribe_mqtt(self):
        """Ensure MQTT client is connected with all required subscriptions."""
        # Subscribe to configuration topics and miniserver startup event
        all_topics = global_config.topics.subscriptions + [
            TOPIC.CONFIG_SET,
            TOPIC.CONFIG_ADD,
            TOPIC.CONFIG_REMOVE,
            TOPIC.CONFIG_UPDATE,
            TOPIC.CONFIG_RESTART,
            TOPIC.CONFIG_GET,
            TOPIC.MINISERVER_STARTUP_EVENT
        ]
        
        try:
            # Connect with all required subscriptions
            await mqtt_client.connect(all_topics, self.miniserver_data_processor.handle_mqtt_message)
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise ConfigError(f"MQTT connection failed: {e}")

    def restart_relay(self):
        os.execv(sys.executable, [sys.executable] + sys.argv)

def main():
    # Initialize logging first
    utils.setup_logging()

    # Report the active build / parser / deps right away, so any later failure
    # can be traced back to the exact runtime configuration.
    utils.log_runtime_environment()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    relay = MQTTRelay()
    try:
        asyncio.run(relay.main())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("MQTT Relay exited")

if __name__ == "__main__":
    main()
