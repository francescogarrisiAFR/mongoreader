# mongoreader 1.0.1

- Modified behaviour of `modules.datasheetDashboardDFgenerator`. Numbers are rounded to a reasonable
number of significant digits.

# mongoreader 1.0.0

Various changes have been made to make the library compatible with _mongomanager_ 2.0.0, and many improvements have also been introduced, as listed below.

### Wafer Collations

Many changes have been made to `waferCollations` in order to improve the code and add functionality. These changes will require updates of the documents already present in the database.

- `waferCollation` now fully exploit the `waferBlueprint` document to retrieve other documents.
- `waferCollation` can now automatically recognize a wafer type from its blueprint, thus sub-classes
such as `waferCollation_Bilbao`, `waferCollation_Cordoba`, etc. are no longer necessary.
- `waferCollation` now support components such as "chips", "testChips", "testCells" and "bars".
- Accordingly, attributes such as `.testCells`, `.testCellsDict`, `.bars`, `.barsDict` have been added to the previous ones.
- `waferCollation` now support blueprints such as "chipBlueprints", "testChipBlueprints", "testCellsBlueprints" and "barBlueprints".
- Accordingly, attributes such as `.testCellBPs`, `.testCellsBPsDict`, `.barBPs`, `.barsBPsDict` have been added to the previous ones.
- Many "collect" methods are now hidden.

Wafer collation now support the retrieve and plots of data from _either_ the test history or the datasheet of components. As such, previous methods have been removed and new specific ones have been introduced in their place.

Additionally, the new "retrieve" and "print" methods return a much wider range of information, which can also be returned as a DataFrame from `pandas`.

- Introduced new plot methods:
    - `plotField()`
    - `plotStatus()`
    - `plotProcessStage()`
- **[N.B.]** Consequently, `.plotChipStatus()` and `.plotAllChipStatus()` have been removed.  
- **[N.B.]** Removed methods:
    - `.retrieveDatasheetData()`
    - `.plotDatasheetData()`
- Introduced new methods to retrieve and plot from the components' test histories:
    - `.retrieveTestResults()`
    - `.plotTestResults()`
    - `.plotAveragedTestResults()`
- Introduced attribute class `Datasheets` for waferCollations.
- Introduced new methods to retrieve and plot data from the components' datasheets:
    - `.Datasheets.retrieveData()`
    - `.Datasheets.retrieveAveragedData()`
    - `.Datasheets.plotData()`
    - `.Datasheets.plotAveragedData()`
- Introduced function `retrieveAllTestResultNames()`.


### GoggleFunctions
- **[N.B.]** The module has been moved in _mongomanager_.

### Modules
- Added Python module modules.py for working with module component.
- Introduced class `moduleCollation` to aggregate the components of a modulator (modulator, COS, chip).
- Introduced function `queryModules()`.
- **TODO** Introduced class `moduleBatch` to aggregate modules from a given batch.

### Connectors
- Created module `connectors`
- Defined function `datasheetDashboardDFgenerator()`

### Other
- `pandas` has been added to the dependencies of `mongomanager`.
- Introduced utility function `averageSubchipScaleDataDict()`, which averages subchip-scale data to chip-scale data.
- Various improvement to documenation strings.
- Many bug fixes.

