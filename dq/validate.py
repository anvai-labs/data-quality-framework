# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Validation utilities for Data Quality Framework."""

import argparse
import sys
import json
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_BUILT_IN_PAYLOAD_KEYS = {
    "custom": "checks",
    "deequ": "checks",
    "dqdl": "ruleset",
    "greatexpectations": "expectations",
    "schemavalidation": "schema",
}


def is_local_config_reference(config_path: str) -> bool:
    """Return whether a configuration can be validated without Spark or I/O."""
    from urllib.parse import urlparse

    return urlparse(config_path).scheme in ("", "file")


def validate_config(config_path: str) -> bool:
    """Validate a HOCON configuration file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        True if configuration is valid, False otherwise.
    """
    try:
        from pyhocon import ConfigFactory
        from urllib.parse import urlparse

        parsed = urlparse(config_path)

        if is_local_config_reference(config_path):
            # Local file
            file_path = (
                config_path.replace("file://", "")
                if parsed.scheme == "file"
                else config_path
            )
            config = ConfigFactory.parse_file(file_path)
        else:
            # Treat an unvalidated remote rule set as unknown, never valid.  A
            # caller that needs a remote source must first materialize and
            # validate an immutable local copy.
            logger.error(
                "Remote config validation is not supported for scheme: %s",
                parsed.scheme,
            )
            return False

        # Check required keys
        if not config.get("dqframework"):
            logger.error("Missing required key: 'dqframework'")
            return False

        dqrules = config.get("dqframework.dqrules", [])
        if not dqrules:
            logger.error("No dqrules defined in configuration")
            return False

        for i, rule in enumerate(dqrules):
            engine = rule.get("engine", None)
            if not engine:
                logger.error(f"Rule {i}: Missing required key 'engine'")
                return False
            payload_key = _BUILT_IN_PAYLOAD_KEYS.get(str(engine).lower())
            if payload_key and not rule.get(payload_key):
                logger.error(
                    "Rule %s: Engine '%s' requires a non-empty '%s' payload",
                    i,
                    engine,
                    payload_key,
                )
                return False
            if payload_key == "ruleset":
                ruleset = rule.get(payload_key)
                if not isinstance(ruleset, str) or not ruleset.strip():
                    logger.error("Rule %s: DQDL ruleset must be a non-empty string", i)
                    return False

        logger.info("Configuration is valid")
        return True

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return False


def main():
    """Main entry point for dq-validate CLI."""
    parser = argparse.ArgumentParser(
        description="Validate Data Quality Framework configuration files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  dq-validate config.conf
  dq-validate file://path/to/config.conf
  dq-validate --format json config.conf
        """,
    )
    parser.add_argument("config", help="Path to HOCON configuration file to validate")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    is_valid = validate_config(args.config)

    if args.format == "json":
        result = {"config": args.config, "valid": is_valid}
        print(json.dumps(result, indent=2))
    else:
        if is_valid:
            print(f"Configuration '{args.config}' is valid.")
        else:
            print(f"Configuration '{args.config}' is invalid.")

    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
