#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from pathlib import Path


def _default_settings_module() -> str:
    project_root = Path(__file__).resolve().parent
    for child in project_root.iterdir():
        if not child.is_dir():
            continue
        if (child / "settings.py").exists() and (child / "__init__.py").exists():
            return f"{child.name}.settings"
    return "config.settings"


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', _default_settings_module())
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
