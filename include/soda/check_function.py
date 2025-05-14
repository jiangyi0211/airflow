def check(scan_name, checks_subpath=None, data_source='retail', project_root='include'):
    from soda.scan import Scan

    # ⚠️ 可选调试信息：打印 soda-core 和 airflow（如可用）的版本
    try:
        from importlib.metadata import version
        soda_version = version("soda-core")
        print(f"✅ soda-core version: {soda_version}")
        airflow_version = version("apache-airflow")
        print(f"ℹ️ apache-airflow version: {airflow_version}")
    except Exception as e:
        print(f"⚠️ Could not determine package version info: {e}")

    print('🚀 Running Soda Scan ...')
    config_file = f'{project_root}/soda/configuration.yml'
    checks_path = f'{project_root}/soda/checks'

    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('❌ Soda Scan failed')

    return result