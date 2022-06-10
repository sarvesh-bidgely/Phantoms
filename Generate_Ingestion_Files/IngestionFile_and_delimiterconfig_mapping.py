from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType

class IngestionFileDelimiterConfigMapping():
    mapping={
        IngestionFileType.EnrollmentFile:"user_creation_parser_delimiter",
        IngestionFileType.InvoiceFile:"invoice_parser_delimiter",
        IngestionFileType.RawFile:"raw_data_parser_delimiter",
        IngestionFileType.OPwrFile:"user_creation_parser_delimiter"
    }

    def __init__(self, file_type):
        self.File_Type=file_type

    def GetFileMappingConfig(self):
        try:
            return IngestionFileDelimiterConfigMapping.mapping[self.File_Type]
        except KeyError:
            raise KeyError(f"The File Type {self.File_Type} is not defined in mapping")



