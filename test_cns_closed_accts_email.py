import csv
import os
import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from cns_closed_accts_email import (
    run,
    send_email_enabled,
    fetch_records,
    process_records,
    update_stdl_userfield,
    write_report_file,
    send_notification_email,
    execute_sql_select,
    is_local_environment,
    send_smtp_request,
)

# Sample test data for person and organization records
SAMPLE_PERS_RECORDS = [
    {
        "ENTITY_NUMBER": 3005329,
        "ACCTNBR": 9351560090,
        "ENTITY_TYPE": "pers",
        "CLOSE_DATE": "07-14-2025",
    },
    {
        "ENTITY_NUMBER": 3826855,
        "ACCTNBR": 9351370359,
        "ENTITY_TYPE": "pers",
        "CLOSE_DATE": "07-14-2025",
    },
]

SAMPLE_ORG_RECORDS = [
    {
        "ENTITY_NUMBER": 1001,
        "ACCTNBR": 9344462412,
        "ENTITY_TYPE": "org",
        "CLOSE_DATE": "07-14-2025",
    },
    {
        "ENTITY_NUMBER": 1002,
        "ACCTNBR": 9351842829,
        "ENTITY_TYPE": "org",
        "CLOSE_DATE": "07-14-2025",
    },
]


def test_run(script_data, mocker):
    """Test the main run function"""
    mock_initialize = mocker.patch(
        "cns_closed_accts_email.initialize",
        return_value=script_data,
    )
    mock_fetch_records = mocker.patch(
        "cns_closed_accts_email.fetch_records",
        return_value=(SAMPLE_PERS_RECORDS, SAMPLE_ORG_RECORDS),
    )
    mock_process_records = mocker.patch(
        "cns_closed_accts_email.process_records",
        return_value=([("3005329", "9351560090", "pers", "07-14-2025", "Success")], []),
    )
    mock_write_report = mocker.patch(
        "cns_closed_accts_email.write_report_file",
        return_value=None,
    )
    mock_send_notification = mocker.patch(
        "cns_closed_accts_email.send_notification_email",
        return_value=None,
    )
    mock_db_close = mocker.patch.object(script_data.dbh, "close", return_value=None)

    # Function under test
    result = run(script_data.apwx)

    # Validate
    assert result is True
    mock_initialize.assert_called_once_with(script_data.apwx)
    mock_fetch_records.assert_called_once_with(script_data)
    mock_process_records.assert_called_once_with(script_data, SAMPLE_PERS_RECORDS, SAMPLE_ORG_RECORDS)
    mock_write_report.assert_called_once()
    mock_send_notification.assert_called_once()
    mock_db_close.assert_called_once()


def test_fetch_records_with_run_date(script_data, mocker):
    """Test fetch_records with RUN_DATE parameter"""
    mock_execute_sql = mocker.patch(
        "cns_closed_accts_email.execute_sql_select",
        return_value=SAMPLE_PERS_RECORDS + SAMPLE_ORG_RECORDS,
    )

    pers_records, org_records = fetch_records(script_data)

    assert len(pers_records) == 2
    assert len(org_records) == 2
    assert pers_records[0]["ENTITY_TYPE"] == "pers"
    assert org_records[0]["ENTITY_TYPE"] == "org"
    mock_execute_sql.assert_called_once()


def test_fetch_records_with_full_cleanup(script_data_full_cleanup, mocker):
    """Test fetch_records with FULL_CLEANUP_YN parameter"""
    mock_execute_sql = mocker.patch(
        "cns_closed_accts_email.execute_sql_select",
        return_value=SAMPLE_PERS_RECORDS + SAMPLE_ORG_RECORDS,
    )

    pers_records, org_records = fetch_records(script_data_full_cleanup)

    assert len(pers_records) == 2
    assert len(org_records) == 2
    mock_execute_sql.assert_called_once()


def test_fetch_records_mutually_exclusive_error(script_data, mocker):
    """Test fetch_records with both RUN_DATE and FULL_CLEANUP_YN set"""
    # Modify script_data to have both parameters set
    script_data.apwx.args.RUN_DATE = "07-23-2025"
    script_data.apwx.args.FULL_CLEANUP_YN = "Y"

    with pytest.raises(Exception, match="Parameter error - IS_FULL_CLEANUP and RUN_DATE params are mutually exclusive"):
        fetch_records(script_data)


def test_fetch_records_no_parameters_error(script_data, mocker):
    """Test fetch_records with neither RUN_DATE nor FULL_CLEANUP_YN set"""
    # Modify script_data to have neither parameter set
    script_data.apwx.args.RUN_DATE = None
    script_data.apwx.args.FULL_CLEANUP_YN = "N"

    with pytest.raises(Exception, match="Parameter error - no RUN_DATE parameter provided, and IS_FULL_CLEANUP not selected"):
        fetch_records(script_data)


def test_process_records_success(script_data, mocker):
    """Test process_records with successful updates"""
    mock_update_pers = mocker.patch(
        "cns_closed_accts_email.update_stdl_userfield",
        return_value=([("3005329", "9351560090", "pers", "07-14-2025", "Success")], []),
    )
    mock_update_org = mocker.patch(
        "cns_closed_accts_email.update_stdl_userfield",
        return_value=([("1001", "9344462412", "org", "07-14-2025", "Success")], []),
    )
    mock_path_exists = mocker.patch("pathlib.Path.exists", return_value=False)

    successes, fails = process_records(script_data, SAMPLE_PERS_RECORDS, SAMPLE_ORG_RECORDS)

    assert len(successes) == 2
    assert len(fails) == 0
    mock_update_pers.assert_called_once()
    mock_update_org.assert_called_once()


def test_process_records_file_exists_error(script_data, mocker):
    """Test process_records when output file already exists"""
    mock_path_exists = mocker.patch("pathlib.Path.exists", return_value=True)

    with pytest.raises(FileExistsError):
        process_records(script_data, SAMPLE_PERS_RECORDS, SAMPLE_ORG_RECORDS)


def test_update_stdl_userfield_success(script_data, mocker):
    """Test update_stdl_userfield with successful updates"""
    mock_cursor = Mock()
    mock_cursor.executemany.return_value = None
    mock_cursor.getbatcherrors.return_value = []
    mock_cursor.rowcount = 2
    mock_cursor.close.return_value = None
    
    mock_db_cursor = mocker.patch.object(script_data.dbh, "cursor", return_value=mock_cursor)
    mock_commit = mocker.patch.object(script_data.dbh, "commit", return_value=None)

    successes, fails = update_stdl_userfield(script_data, SAMPLE_PERS_RECORDS, "persuserfield", "persnbr")

    assert len(successes) == 2
    assert len(fails) == 0
    mock_cursor.executemany.assert_called_once()
    mock_commit.assert_called_once()


def test_update_stdl_userfield_with_errors(script_data, mocker):
    """Test update_stdl_userfield with batch errors"""
    # Create a mock batch error
    mock_error = Mock()
    mock_error.offset = 0
    mock_error.message = "ORA-00001: unique constraint violated"

    mock_cursor = Mock()
    mock_cursor.executemany.return_value = None
    mock_cursor.getbatcherrors.return_value = [mock_error]
    mock_cursor.rowcount = 1
    mock_cursor.close.return_value = None
    
    mock_db_cursor = mocker.patch.object(script_data.dbh, "cursor", return_value=mock_cursor)
    mock_commit = mocker.patch.object(script_data.dbh, "commit", return_value=None)

    successes, fails = update_stdl_userfield(script_data, SAMPLE_PERS_RECORDS, "persuserfield", "persnbr")

    assert len(successes) == 0
    assert len(fails) == 1
    assert fails[0][0] == 3005329  # ENTITY_NUMBER
    assert fails[0][4] == "Fail"


def test_update_stdl_userfield_rptonly_mode(script_data_full_cleanup, mocker):
    """Test update_stdl_userfield in report-only mode"""
    mock_cursor = Mock()
    mock_cursor.executemany.return_value = None
    mock_cursor.getbatcherrors.return_value = []
    mock_cursor.rowcount = 2
    mock_cursor.close.return_value = None
    
    mock_db_cursor = mocker.patch.object(script_data_full_cleanup.dbh, "cursor", return_value=mock_cursor)
    mock_rollback = mocker.patch.object(script_data_full_cleanup.dbh, "rollback", return_value=None)

    successes, fails = update_stdl_userfield(script_data_full_cleanup, SAMPLE_PERS_RECORDS, "persuserfield", "persnbr")

    assert len(successes) == 2
    assert len(fails) == 0
    mock_rollback.assert_called_once()


def test_update_stdl_userfield_empty_records(script_data):
    """Test update_stdl_userfield with empty records"""
    successes, fails = update_stdl_userfield(script_data, [], "persuserfield", "persnbr")

    assert len(successes) == 0
    assert len(fails) == 0


def test_write_report_file(script_data, mocker, tmp_path):
    """Test write_report_file"""
    # Set up temporary output path
    script_data.apwx.args.OUTPUT_FILE_PATH = str(tmp_path)
    script_data.apwx.args.OUTPUT_FILE_NAME = "test_output.csv"
    
    successes = [("3005329", "9351560090", "pers", "07-14-2025", "Success")]
    fails = [("3826855", "9351370359", "pers", "07-14-2025", "Fail")]

    write_report_file(script_data, successes, fails)

    # Verify file was created
    output_file = tmp_path / "test_output.csv"
    assert output_file.exists()

    # Verify content
    with open(output_file, 'r', newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == ['ENTITY_NBR', 'ACCTNBR', 'ENTITY_TYPE', 'CLOSE_DATE', 'RESULT']
        
        # Check success record
        success_row = next(reader)
        assert success_row == ['3005329', '9351560090', 'pers', '07-14-2025', 'Success']
        
        # Check fail record
        fail_row = next(reader)
        assert fail_row == ['3826855', '9351370359', 'pers', '07-14-2025', 'Fail']


def test_send_notification_email_with_failures(script_data, mocker):
    """Test send_notification_email when there are failures"""
    mock_send_email = mocker.patch(
        "cns_closed_accts_email.send_email",
        return_value=(True, "Email Sent"),
    )
    fails = [("3826855", "9351370359", "pers", "07-14-2025", "Fail")]

    send_notification_email(script_data, fails)

    mock_send_email.assert_called_once()


def test_send_notification_email_no_failures(script_data, mocker):
    """Test send_notification_email when there are no failures"""
    mock_send_email = mocker.patch(
        "cns_closed_accts_email.send_email",
        return_value=(True, "Email Sent"),
    )

    send_notification_email(script_data, [])

    mock_send_email.assert_not_called()


def test_send_notification_email_no_recipients(script_data, mocker):
    """Test send_notification_email when there are no email recipients"""
    script_data.apwx.args.EMAIL_RECIPIENTS = None
    fails = [("3826855", "9351370359", "pers", "07-14-2025", "Fail")]

    send_notification_email(script_data, fails)

    # Should not call send_email when no recipients


def test_send_email_enabled_true(script_data):
    """Test send_email_enabled when SEND_EMAIL_YN is Y"""
    assert send_email_enabled(script_data.apwx) is True


def test_send_email_enabled_false(script_data_send_email_n):
    """Test send_email_enabled when SEND_EMAIL_YN is N"""
    assert send_email_enabled(script_data_send_email_n.apwx) is False


def test_execute_sql_select(script_data, mocker):
    """Test execute_sql_select function"""
    mock_cursor = Mock()
    mock_cursor.description = [("ENTITY_NUMBER",), ("ACCTNBR",), ("ENTITY_TYPE",)]
    mock_cursor.fetchall.return_value = [(3005329, 9351560090, "pers")]
    
    mock_db_cursor = mocker.patch.object(script_data.dbh, "cursor", return_value=mock_cursor)

    result = execute_sql_select(script_data.dbh, "SELECT * FROM test_table")

    assert len(result) == 1
    assert result[0]["ENTITY_NUMBER"] == 3005329
    assert result[0]["ACCTNBR"] == 9351560090
    assert result[0]["ENTITY_TYPE"] == "pers"


def test_execute_sql_select_with_params(script_data, mocker):
    """Test execute_sql_select function with parameters"""
    mock_cursor = Mock()
    mock_cursor.description = [("ENTITY_NUMBER",)]
    mock_cursor.fetchall.return_value = [(3005329,)]
    
    mock_db_cursor = mocker.patch.object(script_data.dbh, "cursor", return_value=mock_cursor)

    result = execute_sql_select(script_data.dbh, "SELECT * FROM test_table WHERE id = :id", {"id": 3005329})

    assert len(result) == 1
    assert result[0]["ENTITY_NUMBER"] == 3005329


def test_execute_sql_select_error(script_data, mocker):
    """Test execute_sql_select function with SQL error"""
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = Exception("Database error")
    
    mock_db_cursor = mocker.patch.object(script_data.dbh, "cursor", return_value=mock_cursor)

    with pytest.raises(Exception, match="SQL error = Database error"):
        execute_sql_select(script_data.dbh, "SELECT * FROM test_table")


def test_is_local_environment_true(mocker):
    """Test is_local_environment when AW_HOME is not set"""
    mocker.patch.dict(os.environ, {}, clear=True)
    
    from cns_closed_accts_email import is_local_environment
    assert is_local_environment() is True


def test_is_local_environment_false(mocker):
    """Test is_local_environment when AW_HOME is set"""
    mocker.patch.dict(os.environ, {"AW_HOME": "/path/to/appworx"}, clear=True)
    
    from cns_closed_accts_email import is_local_environment
    assert is_local_environment() is False


def test_send_smtp_request(script_data, mocker):
    """Test send_smtp_request function"""
    mock_smtp = mocker.patch("cns_closed_accts_email.smtplib.SMTP")
    mock_server = Mock()
    mock_smtp.return_value.__enter__.return_value = mock_server
    
    from cns_closed_accts_email import EmailMessage
    email_message = EmailMessage()
    email_message["Subject"] = "Test"
    email_message["From"] = "test@example.com"
    email_message["To"] = "recipient@example.com"
    email_message.set_content("Test content")

    send_smtp_request(script_data.apwx, "test@example.com", "recipient@example.com", email_message)

    mock_server.connect.assert_called_once()
    mock_server.ehlo.assert_called()
    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once()
    mock_server.sendmail.assert_called_once()
