from tethys_sdk.base import TethysAppBase
from tethys_sdk.app_settings import SchedulerSetting


class App(TethysAppBase):
    """
    Tethys app class for Dask Tutorial.
    """

    name = 'Dask Tutorial'
    description = ''
    package = 'dask_tutorial'  # WARNING: Do not change this value
    index = 'home'
    icon = f'{package}/images/dask-logo.png'
    root_url = 'dask-tutorial'
    color = '#c23616'
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def scheduler_settings(self):
        scheduler_settings = (
            SchedulerSetting(
                name='dask_primary',
                description='Scheduler for a Dask distributed cluster.',
                engine=SchedulerSetting.DASK,
                required=True
            ),
        )

        return scheduler_settings