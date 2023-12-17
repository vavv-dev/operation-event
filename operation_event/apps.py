"""
operation_event Django application initialization.
"""

from django.apps import AppConfig


class OperationEventConfig(AppConfig):
    """
    Configuration for the operation_event Django application.
    """

    name = "operation_event"

    plugin_app = {
        "signals_config": {
            "lms.djangoapp": {
                "relative_path": "signals",
            },
            "cms.djangoapp": {
                "relative_path": "signals",
            },
        },
    }
