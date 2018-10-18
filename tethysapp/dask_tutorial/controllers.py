from django.shortcuts import render, reverse
from django.contrib.auth.decorators import login_required
from tethys_sdk.gizmos import Button

from dask.distributed import Client
from tethys_sdk.compute import get_scheduler
from tethys_sdk.jobs import DaskJob
from tethys_sdk.gizmos import JobsTable
from tethysapp.dask_tutorial.job_functions import total, total_future, inc
from django.http.response import HttpResponseRedirect


@login_required()
def home(request):
    """
    Controller for the app home page.
    """
    dask_delayed_button = Button(
        display_text='Dask Delayed Job',
        name='dask_delayed_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Dask Delayed Job'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'status': 'delayed'})
    )

    dask_future_button = Button(
        display_text='Dask Future Job',
        name='dask_future_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Dask Future Job'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'status': 'future'})
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Show All Jobs'
        },
        href=reverse('dask_tutorial:jobs-table')
    )

    context = {
        'dask_delayed_button': dask_delayed_button,
        'dask_future_button': dask_future_button,
        'jobs_button': jobs_button,
    }

    return render(request, 'dask_tutorial/home.html', context)


@login_required()
def run_job(request, status):
    """
    Controller for the app home page.
    """
    if status:
        scheduler = get_scheduler(name='test_scheduler')
        dask = DaskJob(name='test_dask_job', user=request.user, label='test_dask', scheduler=scheduler)

    if status.lower() == 'delayed':
        # Create dask delayed object
        delayed_job = total()

        # Execute future
        dask.execute(delayed_job)

    elif status.lower() == 'future':

        # Get the client to create future
        client = Client(scheduler.host)
        # Create future
        future_job = client.submit(total_future, pure=False)
        # Execute future
        dask.execute(future_job)

    return HttpResponseRedirect(reverse('dask_tutorial:jobs-table'))


@login_required()
def jobs_table(request):
    jobs = DaskJob.objects.filter().order_by('-id')
    # Table View
    jobs_table_options = JobsTable(
        jobs=jobs,
        column_fields=('id', 'name', 'description', 'creation_time'),
        hover=True,
        striped=False,
        bordered=False,
        condensed=False,
        results_url='dask_tutorial:result',
        refresh_interval=1000,
        delete_btn=True,
        show_detailed_status=True,
    )

    home_button = Button(
        display_text='Home',
        name='home_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Home'
        },
        href=reverse('dask_tutorial:home')
    )

    context = {'jobs_table': jobs_table_options, 'home_button': home_button}

    return render(request, 'dask_tutorial/jobs_table.html', context)


@login_required()
def result(request, job_id):
    job = DaskJob.objects.get(id=job_id)

    # Get result and Key
    job_result = job.result()
    key = job.key

    home_button = Button(
        display_text='Home',
        name='home_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Home'
        },
        href=reverse('dask_tutorial:home')
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Show All Jobs'
        },
        href=reverse('dask_tutorial:jobs-table')
    )

    context = {'result': job_result, 'key': key, 'home_button': home_button, 'jobs_button': jobs_button}

    return render(request, 'dask_tutorial/results.html', context)
