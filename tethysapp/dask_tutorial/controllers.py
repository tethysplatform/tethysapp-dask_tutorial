from django.shortcuts import render, reverse
from django.contrib.auth.decorators import login_required
from tethys_sdk.gizmos import Button

import time
import dask
import uuid
from dask.distributed import Client, as_completed
from tethys_sdk.compute import get_scheduler
from tethys_sdk.jobs import DaskJob
from tethys_sdk.gizmos import JobsTable
from tethysapp.dask_tutorial.job_functions import total
from django.http.response import HttpResponseRedirect


@login_required()
def home(request):
    """
    Controller for the app home page.
    """
    dask_button = Button(
        display_text='Dask Job',
        name='dask_button',
        attributes={
            'data-toggle':'tooltip',
            'data-placement':'top',
            'title':'Next'
        },
        href=reverse('dask_tutorial:run-dask')
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'data-toggle':'tooltip',
            'data-placement':'top',
        },
        href=reverse('dask_tutorial:jobs-table')
    )

    context = {
        'dask_button': dask_button,
        'jobs_button': jobs_button,
    }

    return render(request, 'dask_tutorial/home.html', context)


@login_required()
def run_job(request):
    """
    Controller for the app home page.
    """
    status = 'delayed'
    if status.lower() == 'delayed':
        # Create dask delayed object
        delayed_job = total()

        # Create Dask Job
        # scheduler = DaskScheduler(name='test_scheduler', host='tcp://192.168.99.198:8786')
        # scheduler.save()
        scheduler = get_scheduler(name='test_scheduler')
        delayed_dask = DaskJob(name='test_dask_job', user=request.user, label='test_dask', scheduler=scheduler)
        delayed_dask.execute(delayed_job)

    else:
        client = Client('10.0.2.15:8786')
        pass

    return HttpResponseRedirect('../jobs_table')


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
        # results_url='gizmos:results',
        refresh_interval=1000,
        delete_btn=True,
        show_detailed_status=False,
    )

    context = {'jobs_table': jobs_table_options}

    return render(request, 'dask_tutorial/jobs_table.html', context)


@login_required()
def result(request, id):
    job = DaskJob.objects.get(id=id)

    # Get result and Key
    job_result = job.result
    key = job.key
    context = {'result': job_result, 'key': key}

    return render(request, 'dask_tutorial/results.html', context)
