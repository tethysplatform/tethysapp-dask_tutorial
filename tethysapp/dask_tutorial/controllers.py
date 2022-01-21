import random
from django.shortcuts import render, reverse, redirect
from tethys_sdk.permissions import login_required
from django.http.response import HttpResponseRedirect
from django.contrib import messages
from tethys_sdk.gizmos import Button
from tethys_sdk.gizmos import JobsTable
from tethys_compute.models.dask.dask_job_exception import DaskJobException
from tethysapp.dask_tutorial.app import DaskTutorial as app

# get job manager for the app
job_manager = app.get_job_manager()


@login_required()
def home(request):
    """
    Controller for the app home page.
    """
    dask_delayed_button = Button(
        display_text='Dask Delayed Job',
        name='dask_delayed_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Dask Delayed Job'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'job_type': 'delayed'})
    )

    dask_distributed_button = Button(
        display_text='Dask Distributed Job',
        name='dask_distributed_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Dask Future Job'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'job_type': 'distributed'})
    )

    dask_multiple_leaf_button = Button(
        display_text='Dask Multiple Leaf Jobs',
        name='dask_multiple_leaf_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Dask Multiple Leaf Jobs'
        },
        href=reverse('dask_tutorial:run-dask', kwargs={'job_type': 'multiple-leaf'})
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Show All Jobs'
        },
        href=reverse('dask_tutorial:jobs-table')
    )

    context = {
        'dask_delayed_button': dask_delayed_button,
        'dask_distributed_button': dask_distributed_button,
        'dask_multiple_leaf_button': dask_multiple_leaf_button,
        'jobs_button': jobs_button,
    }

    return render(request, 'dask_tutorial/home.html', context)


@login_required()
def run_job(request, job_type):
    """
    Controller for the app home page.
    """
    # Get scheduler from dask_primary setting.
    scheduler = app.get_scheduler(name='dask_primary')

    if job_type.lower() == 'delayed':
        from tethysapp.dask_tutorial.job_functions import delayed_job

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

    elif job_type.lower() == 'distributed':
        from tethysapp.dask_tutorial.job_functions import distributed_job, convert_to_dollar_sign

        # Get the client to create future
        try:
            client = scheduler.client
        except DaskJobException:
            return redirect(reverse('dask_tutorial:error_message'))

        # Create future job instance
        future = distributed_job(client)
        dask = job_manager.create_job(
            job_type='DASK',
            name='dask_distributed',
            user=request.user,
            scheduler=scheduler,
        )
        dask.process_results_function = convert_to_dollar_sign
        dask.execute(future)

    elif job_type.lower() == 'multiple-leaf':
        from tethysapp.dask_tutorial.job_functions import multiple_leaf_job

        # Get the client to create future
        try:
            client = scheduler.client
        except DaskJobException:
            return redirect(reverse('dask_tutorial:error_message'))

        # Create future job instance
        futures = multiple_leaf_job(client)

        # Execute multiple future
        i = random.randint(1, 10000)

        for future in futures:
            i += 1
            name = 'dask_leaf' + str(i)
            dask = job_manager.create_job(
                job_type='DASK',
                name=name,
                user=request.user,
                scheduler=scheduler,
            )
            dask.execute(future)

    return HttpResponseRedirect(reverse('dask_tutorial:jobs-table'))


@login_required()
def jobs_table(request):
    # Using job manager to get all jobs in the database.
    jobs = job_manager.list_jobs(order_by='-id', filters=None)
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
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Home'
        },
        href=reverse('dask_tutorial:home')
    )

    context = {'jobs_table': jobs_table_options, 'home_button': home_button}

    return render(request, 'dask_tutorial/jobs_table.html', context)


@login_required()
def result(request, job_id):
    # Using job manager to get the specified job.
    job = job_manager.get_job(job_id=job_id)

    # Get result and Key
    job_result = job.result
    name = job.name

    home_button = Button(
        display_text='Home',
        name='home_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
            'title': 'Home'
        },
        href=reverse('dask_tutorial:home')
    )

    jobs_button = Button(
        display_text='Show All Jobs',
        name='dask_button',
        attributes={
            'bs-data-toggle': 'tooltip',
            'bs-data-placement': 'top',
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
