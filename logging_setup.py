import logging

# Matches cresproc's own cresproc/logging.py base_fmt exactly, so log lines
# from either repo's tools look the same.
base_fmt = "%(asctime)s %(levelname)s %(name)s"


def init_logging(root_level: str | None, overrides_str: str | None) -> None:
    """Parses granular logging definitions and configures the logging subsystem.

    Same shape as cresproc.logging.init_logging: root_level sets the root
    logger's level (e.g. "INFO", "DEBUG"); overrides_str is a comma-separated
    "logger_name=LEVEL" list for raising or lowering individual loggers below
    the root level (e.g. "botocore=WARNING,myapp.noisy_module=DEBUG").
    """
    if not root_level:
        root_level_num = None
    else:
        root_level_num = getattr(logging, root_level.upper(), None)
        if root_level_num is None:
            logging.warning("Ignored invalid root log level '%s'.", root_level)
    logging.basicConfig(level=root_level_num, format=f"{base_fmt}: %(message)s")

    if not overrides_str:
        return

    pairs = overrides_str.split(",")
    for pair in pairs:
        if "=" not in pair:
            continue

        logger_name, level_name = pair.split("=", 1)
        logger_name = logger_name.strip()
        level_name = level_name.strip().upper()

        if hasattr(logging, level_name):
            target_level = getattr(logging, level_name)
            logging.getLogger(logger_name).setLevel(target_level)
        else:
            logging.warning("Ignored invalid log level '%s' specified for namespace '%s'.", level_name, logger_name)
