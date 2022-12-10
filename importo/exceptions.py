class ImportCommandError(Exception):
    pass


class CommandOptionError(ValueError, ImportCommandError):
    pass


class SkipRow(Exception):
    """
    Raised during processing to indicate to BaseImportCommand.process_row()
    that the current row should be skipped. Usually raised by
    BaseImportCommand.clean().
    """

    pass


class SkipField(Exception):
    """
    Raised by some field class methods to indicate to BaseImportCommand.clean()
    that the importer should not attempt to update the relevant field on the
    model instance.
    """

    pass
