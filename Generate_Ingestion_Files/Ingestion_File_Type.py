import enum

class IngestionFileType(enum.Enum):
    EnrollmentFile=1
    InvoiceFile=2
    RawFile=3
    OPwrFile=4