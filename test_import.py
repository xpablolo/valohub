try:
    from jobs.analytical_report_job import run_analytical_report_job
    print('import ok', run_analytical_report_job)
except Exception as exc:
    print('import failed', exc)
