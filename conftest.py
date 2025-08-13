import os
import pathlib
import pytest

from dataclasses import dataclass
from ..cns_closed_accts_email import (
    AppWorxEnum,
    get_config,
    get_email_template,
    ScriptData,
)


@dataclass
class FakeApwxArgs:
    """Arguments for the fake Apwx"""

    TNS_SERVICE_NAME: str
    CONFIG_FILE_PATH: str
    EFFDATE: str
    FROM_EMAIL_ADDR: str
    MINOR_CODES: str
    OUTPUT_FILE_PATH: str
    OUTPUT_FILE_NAME: str
    SEND_EMAIL_YN: str
    SMTP_SERVER: str
    SMTP_PORT: str
    SMTP_USER: str
    SMTP_PASSWORD: str
    TEST_EMAIL_ADDR: str


@dataclass
class FakeApwx:
    """Fake Apwx class because the real one needs Oracle thick client
    and that won't work on our CICD pipeline"""

    args: FakeApwxArgs


TEST_BASE_PATH = pathlib.Path(os.path.dirname(_file_))

# Multiple structures such as this can be created to have fixtures
# with different behaviors
SCRIPT_ARGUMENTS = {
    str(AppWorxEnum.TNS_SERVICE_NAME): "NON_EXISTING_DB",
    str(AppWorxEnum.CONFIG_FILE_PATH): TEST_BASE_PATH.parent / "config" / "config.yaml",
    str(AppWorxEnum.EFFDATE): "07/23/2025",
    str(AppWorxEnum.FROM_EMAIL_ADDR): "member.communications@firsttechfed.com",
    str(AppWorxEnum.MINOR_CODES): "NACL,NAIL,UAOE,UACL,INRV,INAU,INUA,OVCL,OVOE,UAIL",
    str(AppWorxEnum.OUTPUT_FILE_PATH): TEST_BASE_PATH,
    str(AppWorxEnum.OUTPUT_FILE_NAME): "output.csv",
    str(AppWorxEnum.SEND_EMAIL_YN): "Y",
    str(AppWorxEnum.SMTP_SERVER): "TEST_SMTP_HOST",
    str(AppWorxEnum.SMTP_PORT): "587",
    str(AppWorxEnum.SMTP_USER): "smtp-user",
    str(AppWorxEnum.SMTP_PASSWORD): "smtp-password",
    str(AppWorxEnum.TEST_EMAIL_ADDR): "test_closed_accts_email@firsttechfed.com",
}

SCRIPT_ARGUMENTS_SEND_EMAIL_N = {
    str(AppWorxEnum.TNS_SERVICE_NAME): "NON_EXISTING_DB",
    str(AppWorxEnum.CONFIG_FILE_PATH): TEST_BASE_PATH.parent / "config" / "config.yaml",
    str(AppWorxEnum.EFFDATE): "07/23/2025",
    str(AppWorxEnum.FROM_EMAIL_ADDR): "member.communications@firsttechfed.com",
    str(AppWorxEnum.MINOR_CODES): "NACL,NAIL,UAOE,UACL,INRV,INAU,INUA,OVCL,OVOE,UAIL",
    str(AppWorxEnum.OUTPUT_FILE_PATH): TEST_BASE_PATH,
    str(AppWorxEnum.OUTPUT_FILE_NAME): "output_send_email_n.csv",
    str(AppWorxEnum.SEND_EMAIL_YN): "N",
    str(AppWorxEnum.SMTP_SERVER): "TEST_SMTP_HOST",
    str(AppWorxEnum.SMTP_PORT): "587",
    str(AppWorxEnum.SMTP_USER): "smtp-user",
    str(AppWorxEnum.SMTP_PASSWORD): "smtp-password",
    str(AppWorxEnum.TEST_EMAIL_ADDR): "test_closed_accts_email@firsttechfed.com",
}


def new_fake_apwx(script_args: dict) -> FakeApwx:
    """Creates new fake Apwx object based on script arguments"""
    return FakeApwx(
        args=FakeApwxArgs(
            TNS_SERVICE_NAME=script_args[str(AppWorxEnum.TNS_SERVICE_NAME)],
            CONFIG_FILE_PATH=script_args[str(AppWorxEnum.CONFIG_FILE_PATH)],
            EFFDATE=script_args[str(AppWorxEnum.EFFDATE)],
            FROM_EMAIL_ADDR=script_args[str(AppWorxEnum.FROM_EMAIL_ADDR)],
            MINOR_CODES=script_args[str(AppWorxEnum.MINOR_CODES)],
            OUTPUT_FILE_PATH=script_args[str(AppWorxEnum.OUTPUT_FILE_PATH)],
            OUTPUT_FILE_NAME=script_args[str(AppWorxEnum.OUTPUT_FILE_NAME)],
            SEND_EMAIL_YN=script_args[str(AppWorxEnum.SEND_EMAIL_YN)],
            SMTP_SERVER=script_args[str(AppWorxEnum.SMTP_SERVER)],
            SMTP_PORT=script_args[str(AppWorxEnum.SMTP_PORT)],
            SMTP_USER=script_args[str(AppWorxEnum.SMTP_USER)],
            SMTP_PASSWORD=script_args[str(AppWorxEnum.SMTP_PASSWORD)],
            TEST_EMAIL_ADDR=script_args[str(AppWorxEnum.TEST_EMAIL_ADDR)],
        )
    )


@pytest.fixture(scope="module")
def script_data():
    appworx = new_fake_apwx(SCRIPT_ARGUMENTS)
    config = get_config(appworx)
    return ScriptData(
        apwx=appworx,
        dbh=None,
        config=config,
        email_template=get_email_template(config),
    )


@pytest.fixture(scope="module")
def script_data_send_email_n():
    appworx = new_fake_apwx(SCRIPT_ARGUMENTS_SEND_EMAIL_N)
    config = get_config(appworx)
    return ScriptData(
        apwx=appworx,
        dbh=None,
        config=config,
        email_template=get_email_template(config),
    )
