import csv
import email_validator
import os
import smtplib
import yaml

from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from enum import Enum, auto
from ftfcu_appworx import Apwx, JobTime
from jinja2 import Environment, FileSystemLoader
from oracledb import Connection as DbConnection
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

_version_ = 1.00


class AppWorxEnum(Enum):
    """Define AppWorx arguments here to avoid hard-coded strings"""

    TNS_SERVICE_NAME = auto()
    CONFIG_FILE_PATH = auto()
    EFFDATE = auto()
    FROM_EMAIL_ADDR = auto()
    MINOR_CODES = auto()
    OUTPUT_FILE_PATH = auto()
    OUTPUT_FILE_NAME = auto()
    SEND_EMAIL_YN = auto()
    SMTP_SERVER = auto()
    SMTP_PORT = auto()
    SMTP_USER = auto()
    SMTP_PASSWORD = auto()
    TEST_EMAIL_ADDR = auto()

    def _str_(self):
        return self.name


@dataclass
class ScriptData:
    """Class that holds all the structures and data needed by the script"""

    apwx: Apwx
    dbh: DbConnection
    config: Any
    email_template: Any


def run(apwx: Apwx):
    """The main logic of the script goes here"""
    script_data = initialize(apwx)
    accounts = get_closed_accounts(script_data)
    process_records(script_data, accounts)
    write_audit_log(script_data, accounts)

    return True


def get_apwx() -> Apwx:
    """Creates a new authenticated context for Appworx"""
    return Apwx(["OSIUPDATE", "OSIUPDATE_PW"])


def parse_args(apwx: Apwx) -> Apwx:
    """Parses the arguments to the script"""
    parser = apwx.parser
    parser.add_arg(str(AppWorxEnum.TNS_SERVICE_NAME), type=str, required=True)
    parser.add_arg(
        str(AppWorxEnum.CONFIG_FILE_PATH), type=r"(.yml|.yaml)$", required=True
    )
    parser.add_arg(
        str(AppWorxEnum.EFFDATE), type=r"\d{2}[-\.\\/]\d{2}[-\.\\/]\d{4}", required=True
    )
    parser.add_arg(
        str(AppWorxEnum.FROM_EMAIL_ADDR),
        type=str,
        required=False,
        default="member.communications@firsttechfed.com",
    )
    parser.add_arg(
        str(AppWorxEnum.MINOR_CODES),
        type=str,
        required=False,
        default="NACL,NAIL,UAOE,UACL,INRV,INAU,INUA,OVCL,OVOE,UAIL",
    )
    parser.add_arg(str(AppWorxEnum.OUTPUT_FILE_NAME), type=r"(.csv)$", required=True)
    parser.add_arg(
        str(AppWorxEnum.OUTPUT_FILE_PATH), type=parser.dir_validator, required=True
    )
    parser.add_arg(
        str(AppWorxEnum.SEND_EMAIL_YN), choices=["Y", "N"], default="N", required=False
    )
    parser.add_arg(
        str(AppWorxEnum.SMTP_SERVER),
        type=str,
        required=True,
    )
    parser.add_arg(
        str(AppWorxEnum.SMTP_PORT),
        type=int,
        required=True,
    )
    parser.add_arg(
        str(AppWorxEnum.SMTP_USER),
        type=str,
        required=True,
    )
    parser.add_arg(
        str(AppWorxEnum.SMTP_PASSWORD),
        type=str,
        required=True,
    )
    parser.add_arg(
        str(AppWorxEnum.TEST_EMAIL_ADDR),
        type=str,
        required=False,
    )
    apwx.parse_args()
    return apwx


def initialize(apwx) -> ScriptData:
    """Initialize objects required by the script to call external systems"""
    config = get_config(apwx)
    return ScriptData(
        apwx=apwx,
        dbh=dna_db_connect(apwx),
        config=config,
        email_template=get_email_template(config),
    )


def get_closed_accounts(script_data: ScriptData) -> list[dict]:
    """Get closed accounts starting at a specified date"""
    print("Getting Closed Account List")
    query = script_data.config["get_closed_accounts"]
    effdate = script_data.apwx.args.EFFDATE
    minor_codes = format_minor_codes(script_data.apwx.args.MINOR_CODES)

    query_params = {"effdate": effdate}
    # Not possible to provide list of values in an IN clause as a bind variables.
    # String substitution is the only option here.
    query = query.replace("{{minor_codes}}", minor_codes)
    accounts = execute_sql_select(script_data.dbh, query, query_params)
    for account in accounts:
        print(f"Closed account: {account['ACCTNBR']}")

    print(f"Found {len(accounts)} to process")
    return accounts


def process_records(script_data: ScriptData, accounts: list[dict]):
    """Send emails for each closed account"""
    print("Process Closed Account List")
    email_sent = set()
    for account in accounts:
        account["RESULT"] = ""
        account["EXCPYN"] = False

        if account.get("EMAILADDR") in email_sent:
            account["RESULT"] = "Email Already Sent"
            continue

        if not validate_email(account.get("EMAILADDR")):
            account["RESULT"] = "Email Address Invalid"
            account["EXCPYN"] = True
            continue

        if (account.get("BALANCE") or 0) != 0:
            account["RESULT"] = "Account Has Balance"
            account["EXCPYN"] = True
            continue

        if is_fdi(account):
            account["RESULT"] = "Existing Active 8FDI Note"
            account["EXCPYN"] = True
            continue

        if not account["EXCPYN"]:
            successful, message = send_email(script_data, account)
            email_sent.add(account.get("EMAILADDR"))
            account["EXCPYN"] = not successful
            account["RESULT"] = message


def write_audit_log(script_data: ScriptData, accounts: list[dict]):
    """Generate the output report file"""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Write audit log")
    apwx = script_data.apwx
    output_file_path = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    with open(output_file_path, "w", encoding="utf-8", newline="") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(["CONSUMER CLOSED LOANS EMAIL AUDIT LOG"])
        csv_writer.writerow([f"RUN DATE: {today_date()}"])
        csv_writer.writerow([f"EFFDATE: {apwx.args.EFFDATE}"])
        csv_writer.writerow([])

        csv_writer.writerow(["EMAILS SENT"])
        success_records = list(filter(lambda r: not r["EXCPYN"], accounts))
        write_csv(script_data, csv_writer, success_records)
        csv_writer.writerow([])

        csv_writer.writerow(["EXCEPTIONS"])
        exception_records = list(filter(lambda r: r["EXCPYN"], accounts))
        write_csv(script_data, csv_writer, exception_records)

        csv_writer.writerow(["END"])


def today_date() -> str:
    los_angeles_tz = ZoneInfo("America/Los_Angeles")
    today = datetime.now(los_angeles_tz).date()
    return today.strftime("%m/%d/%Y")


def write_csv(script_data: ScriptData, csv_writer, records: list[dict]):
    if records:
        header = script_data.config["csv_header"]
        csv_writer.writerow(header)
        for record in records:
            csv_writer.writerow([record[field] for field in header])
        csv_writer.writerow([])
    else:
        csv_writer.writerow(["NONE"])
        csv_writer.writerow([])


def validate_email(email: str) -> bool:
    if not email:
        return False

    try:
        email_validator.validate_email(email, check_deliverability=False)
        return True
    except email_validator.EmailNotValidError:
        return False


def is_fdi(account: dict) -> bool:
    """Determines if an account has existing active 8FDI note"""
    if account.get("FDI_NOTECLASSCD") != "8FDI":
        return False

    fdi_inactive_date_str = account.get("FDI_INACTIVE_DATE")
    if not fdi_inactive_date_str:
        return False

    fdi_inactive_date = datetime.strptime(fdi_inactive_date_str, "%m/%d/%Y")
    return fdi_inactive_date >= datetime.now()


def send_email(script_data: ScriptData, account: dict) -> (bool, str):
    apwx = script_data.apwx
    to_address = account.get("EMAILADDR")
    if apwx.args.TEST_EMAIL_ADDR:
        to_address = apwx.args.TEST_EMAIL_ADDR
    from_address = apwx.args.FROM_EMAIL_ADDR

    # Create the email body
    email_content = generate_email_content(script_data, account)
    email_message = generate_email_message(from_address, to_address, email_content)

    # Don't send if we're on local dev env or the SEND_EMAIL_YN parameter is N
    if is_local_environment() or not send_email_enabled(script_data):
        return False, "Email Send Disabled"

    try:
        send_smtp_request(script_data, from_address, to_address, email_message)
        return True, "Email Sent"
    except Exception as e:
        print(f"An exception was encountered sending email to {to_address}.", e)
        return False, "Email Failed"


def generate_email_message(
    from_address: str, to_address: str, email_content: str
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = "Your Closed Automobile Loan"
    message["From"] = f"First Tech Federal Credit Union <{from_address}>"
    message["To"] = to_address
    message.set_content(email_content)
    message.set_type("text/html")
    return message


def generate_email_content(script_data: ScriptData, account: dict) -> str:
    """Generate custom email message with data specific to a member"""
    data = {
        "membername": account["MEMBERNAME"],
        "emaildate": account["EMAILDATE"],
        "year": str(datetime.now().year),
    }
    return script_data.email_template.render(**data)


def send_smtp_request(
    script_data: ScriptData,
    from_address: str,
    to_address: str,
    email_message: EmailMessage,
):
    """Send email request to SMTP server"""
    apwx = script_data.apwx
    smtp_server = apwx.args.SMTP_SERVER
    smtp_port = int(apwx.args.SMTP_PORT)
    smtp_user = apwx.args.SMTP_USER
    smtp_password = apwx.args.SMTP_PASSWORD

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        print(f"Connecting to SMTP server {smtp_server}:{smtp_port}")
        server.connect(smtp_server, smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        print(f"Logging into {smtp_server} as {smtp_user}")
        server.login(smtp_user, smtp_password)
        print(f"Sending email...")
        server.sendmail(from_address, to_address, email_message.as_string())


def format_minor_codes(minor_codes_str: str) -> str:
    """Format the list of minor codes into a form suitable for a SQL IN clause"""
    if not minor_codes_str:
        return ""
    minor_codes = minor_codes_str.split(",")
    minor_codes = map(lambda code: f"'{code.strip().upper()}'", minor_codes)
    return ",".join(minor_codes)


def is_local_environment() -> bool:
    """The absence of AW_HOME means AppWorx is not installed and it's local dev env"""
    return not bool(os.environ.get("AW_HOME"))


def send_email_enabled(script_data: ScriptData) -> bool:
    return script_data.apwx.args.SEND_EMAIL_YN.upper() == "Y"


def dna_db_connect(apwx):
    """Creates a connection to DNA database"""
    return apwx.db_connect(autocommit=False)


def get_config(apwx: Apwx) -> Any:
    """Loads the config YAML file"""
    with open(apwx.args.CONFIG_FILE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_email_template(config: Any) -> Any:
    """Returns the email template object used to generate HTML emails"""
    # Templates are in a 'templates' subfolder relative to the script
    template_directory: str = config["template_directory"]
    template_dir = os.path.join(
        os.path.dirname(os.path.abspath(_file_)), template_directory
    )
    file_loader = FileSystemLoader(template_dir)
    env = Environment(loader=file_loader)
    return env.get_template(config["template_file"])


def execute_sql_select(
    conn: DbConnection,
    sql_statement: str,
    sql_params: Optional[dict] = None,
) -> list[dict]:
    """Executes provided SELECT SQL statement
    Args:
        conn: Database connection object used to connect to DNA.
        sql_statement: The SQL statement to be executed.
        sql_params: Bind variables for the query
    Returns:
        SELECT statements will always return a list of dictionaries.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_statement, sql_params)
            column_names = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(column_names, args))
            return cursor.fetchall()
    except Exception as e:
        raise Exception(f"SQL error = {e}")


if _name_ == "_main_":
    JobTime().print_start()
    run(parse_args(get_apwx()))
    JobTime().print_end()
