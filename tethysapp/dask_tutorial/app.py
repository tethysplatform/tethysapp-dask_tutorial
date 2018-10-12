from tethys_sdk.base import TethysAppBase, url_map_maker

class DaskTutorial(TethysAppBase):
    """
    Tethys app class for Dask Tutorial.
    """

    name = 'Dask Tutorial'
    index = 'dask_tutorial:home'
    icon = 'dask_tutorial/images/icon.gif'
    package = 'dask_tutorial'
    root_url = 'dask-tutorial'
    color = '#f39c12'
    description = 'Place a brief description of your app here.'
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='dask-tutorial',
                controller='dask_tutorial.controllers.home'
            ),
            UrlMap(
                name='run-dask',
                url='dask-tutorial/dask/add',
                controller='dask_tutorial.controllers.run_job'
            ),
            UrlMap(
                name='jobs-table',
                url='dask-tutorial/dask/jobs_table',
                controller='dask_tutorial.controllers.jobs_table'
            ),
            UrlMap(
                name='result',
                url='dask-tutorial/dask/result/{job_id}',
                controller='dask_tutorial.controllers.result'
            ),
        )

        return url_maps
