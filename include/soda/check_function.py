"""Run a Soda scan from an Airflow task (executed inside the soda venv)."""

SODA_ROOT = "/opt/airflow/include/soda"


def check(scan_name: str, checks_subpath: str, data_source: str) -> int:
    from soda.scan import Scan

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(f"{SODA_ROOT}/configuration.yml")
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(f"{SODA_ROOT}/checks/{checks_subpath}")
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    # Fail the task on failed checks or scan errors; warnings are only logged.
    if scan.has_check_fails() or scan.has_error_logs():
        raise ValueError(f"Soda scan {scan_name} failed (exit code {result})")
    return result
