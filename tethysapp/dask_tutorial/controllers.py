from django.shortcuts import render, reverse, redirect
from django.contrib.auth.decorators import login_required
from tethys_sdk.gizmos import Button

from tethys_sdk.compute import get_scheduler
from tethys_sdk.jobs import DaskJob
from dask.distributed import Client
from tethys_sdk.gizmos import JobsTable
from tethysapp.dask_tutorial.job_functions import total, total_future, multiple_future
from django.http.response import HttpResponseRedirect
import random

from tethys_compute.models.dask.dask_job_exception import DaskJobException
from django.contrib import messages

@login_required()
def home(request, error=False):
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

    dask_multiple_future_button = Button(
        display_text='Dask Multiple Future Jobs',
        name='dask_multiple_future_button',
        attributes={
            'data-toggle': 'tooltip',
            'data-placement': 'top',
            'title': 'Dask Multiple Future Jobs'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'status': 'multiple-future'})
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
        'dask_multiple_future_button': dask_multiple_future_button,
    }

    return render(request, 'dask_tutorial/home.html', context)


@login_required()
def run_job(request, status):
    """
    Controller for the app home page.
    """
    if status:
        # Get test_scheduler app. This scheduler needs to be in the database.
        scheduler = get_scheduler(name='test_scheduler')

    if status.lower() == 'delayed':
        # Create a Dask Job with no _process_results_function
        dask = DaskJob(name='dask_delayed', user=request.user, label='test_dask', scheduler=scheduler)

        # Create dask delayed object
        delayed_job = total()

        # Execute future
        dask.execute(delayed_job)

    elif status.lower() == 'future':
        # Create a Dask Job using _process_results_function. We'll use this one for future job scenario
        dask = DaskJob(name='dask_future', user=request.user, label='test_dask', scheduler=scheduler,
                       _process_results_function='tethysapp.dask_tutorial.job_functions.convert_to_dollar_sign')

        # Get the client to create future
        try:
            client = dask.client
        except DaskJobException as e:
            return redirect(reverse('dask_tutorial:error_message'))

        # Create future job instance
        future_job = total_future(client)

        dask.execute(future_job)

    elif status.lower() == 'multiple-future':
        # Get the client to create future
        client = Client(scheduler.host)

        # Create future job instance
        future_job = multiple_future(client)

        # Execute multiple future
        i = random.randint(1, 10000)

        for job in future_job:
            i += 1
            name = 'dask_future' + str(i)
            dask = DaskJob(name=name, user=request.user, label='test_dask', scheduler=scheduler)
            dask.execute(job)

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
    name = job.name

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

    context = {'result': job_result, 'name': name, 'home_button': home_button, 'jobs_button': jobs_button}

    return render(request, 'dask_tutorial/results.html', context)


@login_required()
def error_message(request):
    messages.add_message(request, messages.ERROR, 'Invalid Scheduler!')

    return redirect(reverse('dask_tutorial:home'))

