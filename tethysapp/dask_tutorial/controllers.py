import random
from tethys_sdk.routing import controller
from django.http.response import HttpResponseRedirect
from django.contrib import messages
from tethys_sdk.gizmos import Button
from tethys_sdk.gizmos import JobsTable
from tethys_compute.models.dask.dask_job_exception import DaskJobException
from .app import App

# get job manager for the app
job_manager = App.get_job_manager()


@controller
def home(request):
    """
    Controller for the app home page.
    """

    dask_delayed_button = Button(
        display_text='Dask Delayed Job',
        name='dask_delayed_button',
        attributes={
            'data-bs-toggle': 'tooltip',
            'data-bs-placement': 'top',
            'title': 'Dask Delayed Job'
        },
        href=App.reverse('run_job', kwargs={'job_type': 'delayed'})
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'data-bs-toggle': 'tooltip',
            'data-bs-placement': 'top',
            'title': 'Show All Jobs'
        },
        href=App.reverse('jobs_table')
    )

    context = {
        'dask_delayed_button': dask_delayed_button,
        'jobs_button': jobs_button
    }

    return App.render(request, 'home.html', context)


@controller
def jobs_table(request):
    # Use job manager to get all the jobs.
    jobs = job_manager.list_jobs(order_by='-id', filters=None)

    # Table View
    jobs_table_options = JobsTable(
        jobs=jobs,
        column_fields=('id', 'name', 'description', 'creation_time'),
        hover=True,
        striped=False,
        bordered=False,
        condensed=False,
        results_url=f'{App.package}:result',
        refresh_interval=1000,
        show_detailed_status=True,
    )

    home_button = Button(
        display_text='Home',
        name='home_button',
        attributes={
            'data-bs-toggle': 'tooltip',
            'data-bs-placement': 'top',
            'title': 'Home'
        },
        href=App.reverse('home')
    )

    context = {'jobs_table': jobs_table_options, 'home_button': home_button}

    return App.render(request, 'jobs_table.html', context)


@controller
def result(request, job_id):
    # Use job manager to get the given job.
    job = job_manager.get_job(job_id=job_id)

    # Get result and name
    job_result = job.result
    name = job.name

    home_button = Button(
        display_text='Home',
        name='home_button',
        attributes={
            'data-bs-toggle': 'tooltip',
            'data-bs-placement': 'top',
            'title': 'Home'
        },
        href=App.reverse('home')
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'data-bs-toggle': 'tooltip',
            'data-bs-placement': 'top',
            'title': 'Show All Jobs'
        },
        href=App.reverse('jobs_table')
    )

    context = {
        'result': job_result,
        'name': name,
        'home_button': home_button,
        'jobs_button': jobs_button
    }

    return App.render(request, 'results.html', context)


@controller
def error_message(request):
    messages.add_message(request, messages.ERROR, 'Invalid Scheduler!')
    return App.redirect(App.reverse('home'))

@controller
def run_job(request, job_type):
    """
    Controller for the app home page.
    """
    # Get scheduler from dask_primary setting.
    scheduler = App.get_scheduler(name='dask_primary')

    if job_type.lower() == 'delayed':
        from .job_functions import delayed_job

        # Create dask delayed object
        delayed = delayed_job()
        dask = job_manager.create_job(
            job_type='DASK',
            name='dask_delayed',
            user=request.user,
            scheduler=scheduler,
        )

        # Execute future
        dask.execute(delayed)

    return HttpResponseRedirect(App.reverse('jobs_table'))