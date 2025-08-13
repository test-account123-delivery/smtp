import csv
import os

from pathlib import Path
from ..cns_closed_accts_email import (
    format_minor_codes,
    get_closed_accounts,
    is_fdi,
    run,
    send_email_enabled,
    validate_email,
)

# Get the module name since it is dynamically generated in CICD env
MODULE_NAME = os.path.basename(Path(os.path.dirname(_file_)).parent)

EXPECTED_CLOSED_ACCOUNTS = [
    {
        "ACCTNBR": 9351560090,
        "FDI_NOTECLASSCD": "8FDI",
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "INAU",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "10/25/2024",
        "PERSNBR": 3005329,
        "ORGNBR": None,
        "MEMBERNAME": "Keith Tester0",
        "EMAILADDR": "keith_tester0@gmail.com",
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9351370359,
        "FDI_NOTECLASSCD": None,
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "NACL",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "10/11/2024",
        "PERSNBR": 3826855,
        "ORGNBR": None,
        "MEMBERNAME": "Anuj Tester1",
        "EMAILADDR": "anuj_tester1@gmail.com",
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9344462412,
        "FDI_NOTECLASSCD": None,
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "INUA",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "04/09/2022",
        "PERSNBR": 3657430,
        "ORGNBR": None,
        "MEMBERNAME": "Chasity Tester2",
        "EMAILADDR": "chasity_tester2@gmail.com",
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9351842829,
        "FDI_NOTECLASSCD": "8FDI",
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "INUA",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "11/29/2024",
        "PERSNBR": 3873250,
        "ORGNBR": None,
        "MEMBERNAME": "Christian Tester3",
        "EMAILADDR": "christian_tester3@gmail.com",
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9343100161,
        "FDI_NOTECLASSCD": None,
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "INAU",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "10/06/2021",
        "PERSNBR": 3614006,
        "ORGNBR": None,
        "MEMBERNAME": "Cody Tester4",
        "EMAILADDR": "cody_tester4@gmail.com",
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9351560089,
        "FDI_NOTECLASSCD": "8FDI",
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "INAU",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "10/25/2024",
        "PERSNBR": 3005329,
        "ORGNBR": None,
        "MEMBERNAME": "Stephon Tester5",
        "EMAILADDR": "stephon",  # Invalid email
        "BALANCE": 0,
    },
    {
        "ACCTNBR": 9351370123,
        "FDI_NOTECLASSCD": None,
        "FDI_INACTIVE_DATE": None,
        "MAJOR": "CNS",
        "MINOR": "NACL",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "10/11/2024",
        "PERSNBR": 3826855,
        "ORGNBR": None,
        "MEMBERNAME": "James Tester6",
        "EMAILADDR": "james_tester6@gmail.com",
        "BALANCE": 1000,  # Has a balance
    },
    {
        "ACCTNBR": 9344462456,
        "FDI_NOTECLASSCD": "8FDI",
        "FDI_INACTIVE_DATE": "01/01/2625",  # Existing Active 8FDI Note
        "MAJOR": "CNS",
        "MINOR": "INUA",
        "STATUS": "CLS",
        "CLOSEDATE": "07/14/2025",
        "EMAILDATE": "07/28/2025",
        "CONTRACTDATE": "04/09/2022",
        "PERSNBR": 3657430,
        "ORGNBR": None,
        "MEMBERNAME": "Kyle Tester7",
        "EMAILADDR": "kyle_tester7@gmail.com",
        "BALANCE": 0,
    },
]


def test_run(script_data, mocker):
    mock_initialize = mocker.patch(
        f"{MODULE_NAME}.cns_closed_accts_email.initialize",
        return_value=script_data,
    )
    mock_get_closed_accounts = mocker.patch(
        f"{MODULE_NAME}.cns_closed_accts_email.get_closed_accounts",
        return_value=EXPECTED_CLOSED_ACCOUNTS,
    )
    mock_send_email = mocker.patch(
        f"{MODULE_NAME}.cns_closed_accts_email.send_smtp_request",
        return_value=None,
    )
    mocker.patch(
        f"{MODULE_NAME}.cns_closed_accts_email.is_local_environment",
        return_value=False,
    )

    # Function under test
    assert run(script_data.apwx) is True

    # Validate
    mock_initialize.assert_called_once()
    mock_get_closed_accounts.assert_called_once()

    # There are 8 total records, 3 of which contains exceptions
    assert mock_send_email.call_count == 5

    # Validate the csv file
    _validate_report_file(script_data)


def _validate_report_file(script_data):
    """Helper function to validate actual CSV output file"""
    apwx = script_data.apwx
    csv_header = script_data.config["csv_header"]

    # Open the csv file that was generated by the test
    output_file = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        csv_reader = csv.reader(f)
        assert next(csv_reader) == ["CONSUMER CLOSED LOANS EMAIL AUDIT LOG"]
        assert next(csv_reader)[0].startswith("RUN DATE:")
        assert next(csv_reader) == ["EFFDATE: 07/23/2025"]
        next(csv_reader)

        # Validate accounts that emails were sent out
        assert next(csv_reader) == ["EMAILS SENT"]
        assert next(csv_reader) == csv_header
        for i in range(0, 5):
            row = next(csv_reader)
            for csv_field_index, field in enumerate(csv_header):
                assert row[csv_field_index] == str(EXPECTED_CLOSED_ACCOUNTS[i][field])

        # Skip empty lines
        next(csv_reader)
        next(csv_reader)

        # Validate accounts with errors
        assert next(csv_reader) == ["EXCEPTIONS"]
        assert next(csv_reader) == csv_header
        for i in range(5, 8):
            row = next(csv_reader)
            for csv_field_index, field in enumerate(csv_header):
                assert row[csv_field_index] == str(EXPECTED_CLOSED_ACCOUNTS[i][field])

        next(csv_reader)
        assert next(csv_reader) == ["END"]


def test_get_closed_accounts(script_data, mocker):
    mock_execute_sql_select = mocker.patch(
        f"{MODULE_NAME}.cns_closed_accts_email.execute_sql_select",
        return_value=EXPECTED_CLOSED_ACCOUNTS,
    )
    results = get_closed_accounts(script_data)
    assert results == EXPECTED_CLOSED_ACCOUNTS
    mock_execute_sql_select.assert_called_once()


def test_is_fdi(script_data):
    assert is_fdi(EXPECTED_CLOSED_ACCOUNTS[7]) is True
    # is FDI but FDI_INACTIVE_DATE is null
    assert is_fdi(EXPECTED_CLOSED_ACCOUNTS[0]) is False
    # No FDI info at all
    assert is_fdi(EXPECTED_CLOSED_ACCOUNTS[1]) is False


def test_send_email_enabled_true(script_data):
    assert send_email_enabled(script_data) is True


def test_send_email_enabled_false(script_data_send_email_n):
    assert send_email_enabled(script_data_send_email_n) is False


def test_format_minor_codes():
    # All lower case minor codes
    assert (
        format_minor_codes("nacl,nail,uaoe,uacl,inrv,inau")
        == "'NACL','NAIL','UAOE','UACL','INRV','INAU'"
    )
    # All upper case minor codes
    assert (
        format_minor_codes("NACL,NAIL,UAOE,UACL,INRV,INAU")
        == "'NACL','NAIL','UAOE','UACL','INRV','INAU'"
    )
    # Messy white spaces
    assert (
        format_minor_codes(" nacl,nail ,  uaoe   , uacl ,\tinrv\n,\ninau\t")
        == "'NACL','NAIL','UAOE','UACL','INRV','INAU'"
    )
    # Empty input string
    assert format_minor_codes("") == ""


def test_validate_email():
    assert validate_email("test@firsttechfed.com") is True
    assert validate_email("@firsttechfed.com") is False
    assert validate_email("test@") is False
    assert validate_email("test@firsttechfed") is False
    assert validate_email("test") is False
    assert validate_email("") is False
    assert validate_email(None) is False
