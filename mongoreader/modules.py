import mongomanager as mom
from mongomanager import log, isID
from mongoutils import queryUtils as qu
import mongoreader.core as c
import mongoreader.errors as e
import mongoreader.datasheets as ds
from pandas import DataFrame

def queryModuleNames(conn, *strings, batch:str = None, printNames:bool = True) -> list:
    """Queries and prints on console the components which conain "module" and
    any other "string" in their name (case insensitive) in their name.

    Suppress print on screen with printNames = False.
    """

    strings = ['module'] + [s for s in strings]

    query = qu.regex('name', strings, caseSensitive=False)
    if batch is not None:
        query['batch'] = batch
        
    cmps = mom.component.query(conn, query, projection={'name': 1, '_id': 1}, returnType='dictionary')

    names = []
    IDs = []
    for cmp in cmps:
        name = cmp.get('name')
        ID = cmp.get('_id')
        if name is not None:
            names.append(name)
        if ID is None:
            IDs.append('<No ID>')
        else:
            IDs.append(mom.toStringID(ID))

    if printNames:
        for index, (ID, name) in enumerate(zip(IDs, names)):
            print(f'[{index:3}] ID: {ID} :: "{name}"')
    
    return names


def queryModuleBatches(connection, moduleBlueprint_orID = None,
                       *,
                       regexString:str = None,
                       verbose:bool = True) -> list:
    """Given a module blueprint or ID, this method queries the database and
    returns the list of batches whose components blueprint is the one passed.

    If the blueprint is not passed, a (long) query on all the components is
    performed, returning all the values found for the "batch" field.

    Args:
        connection (mom.connection): The connection object to the MongoDB
            server.
        moduleBlueprint_orID (mom.blueprint | ID, optional): The blueprint.

    Keyword Args:
        regexString (str, optional): If passed, the collected batch strings
            are matched against this regex pattern. Defaults to None.
        verbose (bool): If False, logging output is suppressed. Defaults to
            True.

    Returns:
        list[str] | None: The batches found, or None if nothing is found.
    """

    if moduleBlueprint_orID is None:
        log.warning('No blueprint or ID passed. Querying for all batches may be slow.')
        bpID = None
    
    else:# Extracting the ID from the blueprint
        bpID = mom.classID_orID(moduleBlueprint_orID)

    query = {}

    if bpID is not None:
        query['blueprintID'] = bpID
    
    if regexString is not None:
        query = {**query, **qu.regex('batch', regexString)}


    cmps = mom.component.query(connection, query,
                                projection={'batch': 1},
                                returnType='dictionary',
                                verbose = verbose)
    
    batches = list(set([cmp.get('batch') for cmp in cmps]))
    if None in batches: batches.remove(None)

    if batches == []:
        return None
    
    return batches

class moduleCollation(c.collation):
    
    def __init__(self, connection:mom.connection, module,
                 *,
                 moduleRegexSearch:bool = True,
                 moduleValidationCriterion:callable= None,
                 moduleBlueprintValidationCriterion:callable = None,
                 COSvalidationCriterion:callable= None,
                 COSblueprintValidationCriterion:callable = None,
                 chipValidationCriterion:callable= None,
                 chipBlueprintValidationCriterion:callable = None,
                 collectBlueprints:bool = True,
                 verbose:bool = True,
                 ):
        """Initialization method for the moduleCollation class.

        This method attempts to retrieve the module, COS and chip components/
        blueprints from "module".

        "module" can either be a string, and ID, or a component.
        In the two former cases, the method will attempt a query on the
        database.

        The [name]ValidationCriterion arguments are functions that can be
        used to validate the component/blueprint once it is imported from the
        database.

        Apart from the module, this method will not raise Exceptions if the
        search fails, but it will only issue warnings.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            module (mongomanager.component | str | ID): The module used for
                reference
            moduleRegexSearch (bool, optional): If True, when "module" is a
                string it is considered a regex pattern (case insensitive).
                Notice that parenthesis in the pattern have a specific regex
                behaviour! This is a reason why you may want to put choose
                regex = False. Defaults to True.
            moduleValidationCriterion (callable, optional): See above. Defaults
                to None.
            moduleBlueprintValidationCriterion (callable, optional): See above.
                Defaults to None.
            COSvalidationCriterion (callable, optional): See above. Defaults to
                None.
            COSblueprintValidationCriterion (callable, optional): See above.
                Defaults to None.
            chipValidationCriterion (callable, optional): See above. Defaults to
                None.
            chipBlueprintValidationCriterion (callable, optional): See above.
                Defaults to None.
        """        
        
        if not isinstance(connection, mom.connection):
            raise TypeError('"connection" must be a mongomanager connection object.')

        self.connection = connection

        # Components
        self.module = None
        self.COS = None
        self.chip = None

        # Blueprints
        self.moduleBlueprint = None
        self.COSblueprint = None
        self.chipBlueprint = None

        self._collectDocuments(connection, module,
                            moduleValidationCriterion,
                            moduleBlueprintValidationCriterion,
                            COSvalidationCriterion,
                            COSblueprintValidationCriterion,
                            chipValidationCriterion,
                            chipBlueprintValidationCriterion,
                            moduleRegexSearch,
                            collectBlueprints,
                            verbose = verbose)

    def __repr__(self):

        name = self.module.name
        if name is None: name = '<Name not found>'

        return f'Module collation "{name}"'



    # --- collect methods ---

    def _collectDocuments(self, connection:mom.connection, module,

                            moduleValidationCriterion,
                            moduleBlueprintValidationCriterion,
                            COSvalidationCriterion,
                            COSblueprintValidationCriterion,
                            chipValidationCriterion,
                            chipBlueprintValidationCriterion,
                            moduleRegexSearch:bool = True,
                            collectBlueprints:bool = True,
                            *,
                            verbose:bool = True):
        """Collects all the components and blueprints associated to the module.

        Args:
            connection (mom.connection): _description_
            module (_type_): _description_
        """

        with mom.logMode(mom.log, 'IMPORTANT'):
            with mom.opened(connection):

                self.module = self._collectModule(connection,
                                            module,
                                            moduleValidationCriterion,
                                            regex = moduleRegexSearch,
                                            verbose = verbose)
                
                self.COS = self._collectCOS(connection,
                                            self.module,
                                            COSvalidationCriterion,
                                            verbose = verbose)
                
                self.chip = self._collectChip(connection,
                                            self.COS,
                                            chipValidationCriterion,
                                            verbose = verbose)

                if collectBlueprints is False:

                    self.moduleBlueprint = None
                    self.COSblueprint = None
                    self.chipBlueprint = None

                else:
                            
                    self.moduleBlueprint = self._collectModuleBlueprint(connection,
                                                self.module,
                                                moduleBlueprintValidationCriterion,
                                                verbose = verbose)
                    
                    self.COSblueprint = self._collectCOSblueprint(connection,
                                        self.COS,
                                        COSblueprintValidationCriterion,
                                        verbose = verbose)

                    self.chipBlueprint = self._collectChipBlueprint(connection,
                                        self.chip,
                                        chipBlueprintValidationCriterion,
                                        verbose = verbose)

        mom.log.info(f'Collected documents.')
        

    @staticmethod
    def _collectModule(connection:mom.connection, module,
                      moduleValidationCriterion:callable = None,
                      regex:bool = True,
                      *,
                      verbose:bool = True) -> mom.component:
        """Import the module from the server.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            module (component | str | ID): The module to be collected.
                If module is a component, it is assumed that it is the module
                to be collected and is returned.
                If module is a string, a regex-query on the server is performed.
                If module is ID, the document with that ID is returned.
            database (str, optional): If not None, query/imports are performed
                on this database. Defaults to None.
            collection (str, optional): If not None, query/imports are performed
                on this collection. Defaults to None.
            moduleValidationCriterion (callable, optional): A function of the
                module that returns True/False to check that the component is
                valid. Defaults to None.

        Raises:
            mom.DocumentNotFound: If the import/query fails or if the module
                does not pass the validation criterion.

        Returns:
            mongomanager.core.component: The collected module
        """        
        
        # component
        if isinstance(module, mom.component):
            mod = module

        # ID (ObjectID or string)
        elif isID(module):

            mod = mom.importComponent(module, connection)
            
            if mod is None:
                raise mom.DocumentNotFound(f'Could not import module from ID "{module}".')

        # module name (string)
        elif isinstance(module, str):

            if regex:
                query = qu.regex('name', module, caseSensitive=False)
            else:
                query = {'name': module}
            
            mod = mom.component.queryOne(connection, query, returnType='component', verbose = False)
            
            if mod is None:
                raise mom.DocumentNotFound(f'Could not query module "{module}".')

        else:
            raise TypeError(f'"module" is not a component, a string or an ID (it is {type(module)}).')
        
        if moduleValidationCriterion is not None:
            if not moduleValidationCriterion(mod):
                raise mom.DocumentNotFound(f'The collected module "{module.name}" did not pass the validation criterion.')
        
        if verbose: mom.log.important(f'Collected module "{mod.name}".')
        return mod
    

    @staticmethod
    def _collectCOS(connection:mom.connection, module:mom.component,
                    COSvalidationCriterion:callable = None,
                      *,
                      verbose:bool = True) -> mom.component:
        """Returns the COS associated to the module.

        It is assumed the COS is the first of the inner components of the
        module.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            module (mongomanager.component): The module whose COS is to
                be retrieved.
            COSvalidationCriterion (callable, optional): A function of the
                COS that returns True/False to check that the component is
                valid. Defaults to None.

        Raises:
            mom.DocumentNotFound: If the import fails or if the component
                does not pass the validation criterion.
                
        Returns:
            mongomanager.component: The collected COS component."""
        
        cmps = module.InnerComponents.retrieveElements(connection, verbose = False)
        
        if cmps is None:
            if verbose: mom.log.warning(f'Could not retrieve inner components from module "{module.name}".')
            return None
        
        COS = cmps[0]

        if COSvalidationCriterion is not None:
            if not COSvalidationCriterion(COS):
                if verbose: mom.log.warning(f'The collected COS component "{COS.name}" did not pass the validation criterion.')
                return None
        
        mom.log.important(f'Collected COS "{COS.name}".')
        return COS


    @staticmethod
    def _collectChip(connection:mom.component, COS:mom.component,
                     chipValidationCriterion:callable = None,
                      *,
                      verbose:bool = True) -> mom.component:
        """Returns the chip associated to the COS.

        It is assumed the chip is the first of the inner components of the
        COS.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            COS (mongomanager.component): The COS whose chip is to
                be retrieved.
            chipValidationCriterion (callable, optional): A function of the
                chip that returns True/False to check that the component is
                valid. Defaults to None.

        Raises:
            mom.DocumentNotFound: If the import fails or if the component
                does not pass the validation criterion.
                
        Returns:
            mongomanager.component: The collected COS component."""
        
        if COS is None: # Not found by _collectCOS()
            mom.log.warning('I cannot collect the chip because COS was not collected.')
            return None

        cmps = COS.InnerComponents.retrieveElements(connection, verbose = False)
        
        if cmps is None:
            if verbose: mom.log.warning(f'Could not retrieve inner components from COS "{COS.name}".')
            return None
        
        chip = cmps[0]

        if chipValidationCriterion is not None:
            if not chipValidationCriterion(COS):
                if verbose: mom.log.warning(f'The collected chip "{chip.name}" did not pass the validation criterion.')
                return None
        
        mom.log.important(f'Collected chip "{chip.name}".')
        return chip
    

    @staticmethod
    def _collectModuleBlueprint(connection:mom.connection, module:mom.component,
                    moduleBlueprintValidationCriterion:callable = None,
                    *,
                    verbose:bool = True) -> mom.blueprint:
        """Returns the blueprint associated to the module.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            module (mongomanager.component): The module whose blueprint is to
                be retrieved.
            moduleBlueprintValidationCriterion (callable, optional): A function
                of the moduleBlueprint that returns True/False to check that the
                blueprint is valid. Defaults to None.

        Raises:
            mom.DocumentNotFound: If the import fails or if the blueprint
                does not pass the validation criterion.
                
        Returns:
            mongomanager.blueprint: The collected blueprint.
        """

        try:
            bp = module.retrieveBlueprint(connection, verbose = False)
        except Exception:
            if verbose: log.warning(f'Could not retrieve the blueprint associated to module "{module.name}".')
            return None
    
        if moduleBlueprintValidationCriterion is not None:
            if not moduleBlueprintValidationCriterion(bp):
                raise mom.DocumentNotFound(f'The collected module blueprint "{bp.name}" did not pass the validation criterion.')

        if verbose: mom.log.important(f'Collected module blueprint "{bp.name}".')
        return bp
    

    @staticmethod
    def _collectCOSblueprint(connection:mom.connection, COS:mom.component,
                    COSblueprintValidationCriterion:callable = None,
                    *,
                    verbose:bool = True) -> mom.blueprint:
        """Returns the COS blueprint associated to the COS, or None if it is
        not found or if it does not pass the validation criterion.
         
        It does not raise DocumentNotFound, but raises a warning instead.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            COS (mongomanager.component): The COS whose blueprint is to be
                retrieved.
            COSblueprintValidationCriterion (callable, optional): A function
                of the COS blueprint that returns True/False to check that the
                blueprint is valid. Defaults to None.

        Returns:
            mongomanager.blueprint | None: The collected blueprint.
        """

        if COS is None: # Not found by _collectCOS()
            if verbose: mom.log.warning('I cannot collect the COS blueprint because the COS was not collected.')
            return None

        try:
            bp = COS.retrieveBlueprint(connection)
        except Exception:
            if verbose: log.warning(f'Could not retrieve the blueprint associated to COS "{COS.name}".')
            return None
        
        if bp is None:
            if verbose: log.warning(f'Could not retrieve the blueprint associated to COS "{COS.name}".')
            return None
    
        if COSblueprintValidationCriterion is not None:
            if not COSblueprintValidationCriterion(bp):
                if verbose: log.warning(f'The collected COS blueprint "{COS.name}" did not pass the validation criterion.')
                return None

        if verbose: mom.log.important(f'Collected COS blueprint "{bp.name}".')
        return bp


    @staticmethod
    def _collectChipBlueprint(connection:mom.connection, chip:mom.component,
                    chipBlueprintValidationCriterion:callable = None,
                    *,
                    verbose:bool = True) \
                                -> mom.blueprint:
        """Returns the chip blueprint associated to the chip, or None if it is
        not found or if it does not pass the validation criterion.
         
        It does not raise DocumentNotFound, but raises a warning instead.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            chip (mongomanager.component): The chip whose blueprint is to be
                retrieved.
            chipBlueprintValidationCriterion (callable, optional): A function
                of the chip blueprint that returns True/False to check that the
                blueprint is valid. Defaults to None.

        Returns:
            mongomanager.blueprint | None: The collected blueprint.
        """

        if chip is None: # Not found by _collectChip()
            if verbose: mom.log.warning('I cannot collect the chip blueprint because the chip was not collected.')
            return None

        try:
            bp = chip.retrieveBlueprint(connection)
        except Exception:
            if verbose: log.warning(f'Could not retrieve the blueprint associated to chip "{chip.name}".')
            return None
        
        if bp is None:
            if verbose: log.warning(f'Could not retrieve the blueprint associated to chip "{chip.name}".')
            return None
    
        if chipBlueprintValidationCriterion is not None:
            if not chipBlueprintValidationCriterion(bp):
                if verbose: log.warning(f'The collected chip blueprint "{bp.name}" did not pass the validation criterion.')
                return None

        if verbose: mom.log.important(f'Collected chip blueprint "{bp.name}".')
        return bp
    

    # --- print methods ---

    @staticmethod
    def _collectDashboardInfo_component(component:mom.component):

        if component is None:
            return "<not found>", "<not found>", "<not found>", "<not found>", "<not found>"

        name = component.name
        ID = str(component.ID)
        status = component.status
        stage = component.processStage
        indicators = component.indicatorsString()

        if name is None:
            name = '<not found>'
        else:
            name = f'"{name}"'
        if ID is None: ID = '<not found>'
        if status is None:
            status = '<not found>'
        else:
            status = f'"{status}"'
        if stage is None:
            stage = '<not found>'
        else:
            stage = f'"{stage}"'    
        if indicators is None: indicators = '<not found>'

        return name, ID, status, stage, indicators
    
    @staticmethod
    def _collectDashboardInfo_blueprint(blueprint:mom.component):

        if blueprint is None:
            return "<not found>", "<not found>"

        name = blueprint.name
        ID = str(blueprint.ID)

        if name is None:
            name = '<not found>'
        else:
            name = f'"{name}"'
        if ID is None: ID = '<not found>'

        return name, ID


    def _dashboardString(self):

        def _combine(first, other):
            first = first+'   '
            return f'{first:>20}' + ''.join([f'{s:50}' for s in other])

        moduleInfo = self._collectDashboardInfo_component(self.module)
        COSinfo = self._collectDashboardInfo_component(self.COS)
        chipInfo = self._collectDashboardInfo_component(self.chip)

        moduleBpInfo = self._collectDashboardInfo_blueprint(self.moduleBlueprint)
        COSbpinfo = self._collectDashboardInfo_blueprint(self.COSblueprint)
        chipBpInfo = self._collectDashboardInfo_blueprint(self.chipBlueprint)

        names, IDs, stati, stages, allIndicators = zip(moduleInfo, COSinfo, chipInfo)

        dashboard_cmps = '\n'.join([
            _combine('', ['Module', 'COS', 'Chip']),
            _combine('name:', names),
            _combine('ID:', IDs),
            _combine('status:', stati),
            _combine('process stage:', stages),
            _combine('indicators:', allIndicators),
        ])

        names, IDs = zip(moduleBpInfo, COSbpinfo, chipBpInfo)

        dashboard_bps = '\n'.join([
            _combine('', ['Module blueprint', 'COS blueprint', 'Chip blueprint']),
            _combine('name:', names),
            _combine('ID:', IDs),
        ])

        dashboard = dashboard_cmps + '\n\n' + dashboard_bps

        return dashboard


    def printDashboard(self):
        """Prints to console a dashboard with syntetic information regarding the
        module, COS and chip components and their blueprint."""

        with mom.logMode(mom.log, 'ERROR'):
            print(self._dashboardString())


    # --- other methods ---

    pass



class _Datasheets(ds._DatasheetsBaseClass):
    """Attribute class to apply Datasheet methods to wafer collations"""
    

    def printHelpInfo(self):
        raise NotImplementedError('Not yet implemented.')

    def retrieveData(self,
                    resultNames:list = None,
                    requiredTags:list = None,
                    tagsToExclude:list = None,
                    locations:list = None,
                    *,
                    returnDataFrame:bool = False,
                    datasheetIndex:int = None,
                ):
        """This method can be used to retrieve data from datasheets defined
        for the components of the module batch.

        The argumetns can be used to change what results are collected, as
        described below.

        Args:
            resultNames (list, optional): If passed, results whose name is not
                listed here are ignored.
            requiredTags (list, optional): If passed, results tags must contain
                those listed here to be collected.
            tagsToExclude (list, optional): If passed, results whose tags are
                among these are not collected. Defaults to None.
            locations (list, optional): If passed, the result location must be
                among these for it to be collected. Defaults to None.

        Keyword arguments (**kwargs):
            returnDataFrame (bool, optional): If True, results are returned
                as a pandas DataFrame instead of a list of dictionaries.
                Defaults to False.
            datasheetIndex (int, optional): If passed, the datasheet indexed
                by datasheetIndex is passed. See mongomanager.component for
                more info. Defaults to None.

        Returns:
            List[dict] | pandas.DataFrame: The collected results.
        """        

        scoopedResults = super().retrieveData(
                self._obj.modules,
                resultNames,
                requiredTags,
                tagsToExclude,
                locations,
                returnDataFrame = returnDataFrame,
                datasheetIndex = datasheetIndex)

        if scoopedResults is None:
            return None

        if returnDataFrame:
            scoopedResults.insert(0, "batch", len(scoopedResults)*[self._obj.batch])

        else:
            additionalInfo = {
                    'batch': self._obj.batch,
                }
            scoopedResults = [{**additionalInfo, **res} for res in scoopedResults]
            
        return scoopedResults


@ds._attributeClassDecoratorMaker(_Datasheets)
class moduleBatch:

    def __init__(self, connection, batch:str, regexStrings:str = None,
                 *,
                 verbose:bool = True):

        self.batch = batch
        self._modules = None
        self._moduleCollations = None

        self._modules, self._moduleCollations = \
            self._collectComponents(connection, batch, regexStrings)
        
        self._COSs = [mc.COS for mc in self._moduleCollations]
        self._chips = [mc.chip for mc in self._moduleCollations]

        if verbose:
            mom.log.important(f'Collected {self._countList(self._modules)} modules')
            mom.log.important(f'Collected {self._countList(self._COSs)} COSs')
            mom.log.important(f'Collected {self._countList(self._chips)} chips')

            if self._modules is not None:
                if self._COSs is not None and len(self._COSs) != len(self._modules):
                    log.warning(f'Collected {len(self._modules)} modules but only {len(self._COSs)} COSs.')
            
            if self._COSs is not None:
                if self._chips is not None and len(self._chips) != len(self._COSs):
                    log.warning(f'Collected {len(self._COSs)} modules but only {len(self._chips)} chips.')

        moduleBPs, COSbps, chipBPs = self._collectBlueprints(connection)
        self.moduleBPs = moduleBPs
        self.COSbps = COSbps
        self.chipBPs = chipBPs

        if verbose:
            mom.log.important(f'Collected {self._countList(self.moduleBPs)} module blueprints')
            mom.log.important(f'Collected {self._countList(self.COSbps)} COS blueprints')
            mom.log.important(f'Collected {self._countList(self.chipBPs)} chip blueprints')


    @staticmethod
    def _countList(l):
        """Counts the non-None elements in a list."""

        if l is None: return 0

        counted = [el for el in l if el is not None]
        return len(counted)


    
    @property
    def modules(self):
        return self._modules
    
    @property
    def COSs(self):
        return self._COSs
    
    @property
    def chips(self):
        return self._chips

    def printModulesStatus(self):

        for index, module in enumerate(self.modules):
            nameStr = f'"{module.name}"'
            statusStr = module.getField('status', '<No status>', verbose = None)
            print(f'[{index:3}] {nameStr:30} :: {statusStr}')

    def printModulesProcessStage(self):

        for index, module in enumerate(self.modules):
            nameStr = f'"{module.name}"'
            statusStr = module.getField('processStage', '<No proc. stage>', verbose = None)
            print(f'[{index:3}] {nameStr:30} :: {statusStr}')


    def dashboard(self, returnDataFrame:bool = False) -> list:
        """Returns a list of dictionaries, each contiaining information
        regarding the components of the wafer collation.

        If returnDataFrame is True, a pandas dataframe is returned instead.

        Each dictionary is in the form:

        >>> {
        >>>     'name': <str>,
        >>>     'ID': <ID>,
        >>>     'componentType': <str> ("module", "COS", "chip")
        >>>     'processStage': <str>,
        >>>     'status': <str>,
        >>> }

        Returns:
            list[dict] | DataFrame: The list of dictionaries described above,
                or a pandas DataFrame.
        """

        def _dashboardDict(component):
            """N.B. Lacks 'componentType' key."""

            if component is None:
                return {key: None for key in ['name', 'ID', 'processStage', 'status']}

            return {
                'name': component.name,
                'ID': component.ID,
                'processStage': component.getField('processStage', verbose = False),
                'status': component.getField('status', verbose = False),
            }

        dashboard = []

        # Modules
        cmps = self.modules
        if cmps is None: cmps = []
        for cmp in cmps:
            dashboard.append({**{'componentType': 'module', **_dashboardDict(cmp)}})

        # COSs
        cmps = self.COSs
        if cmps is None: cmps = []
        for cmp in cmps:
            dashboard.append({**{'componentType': 'COS', **_dashboardDict(cmp)}})

        # chips
        cmps = self.chips
        if cmps is None: cmps = []
        for cmp in cmps:
            dashboard.append({**{'componentType': 'chip', **_dashboardDict(cmp)}})

        if dashboard == []:
            return None

        if returnDataFrame:

            dataFrameDict = {key: [] for key in ['componentType', 'name', 'ID', 'processStage', 'status']}
            for dict in dashboard:
                for key in dataFrameDict:
                    dataFrameDict[key].append(dict.get(key))

            return DataFrame(dataFrameDict)
        
        return dashboard


    @staticmethod
    def _cmpDashboardString(cmp):

        if cmp is None:
            IDstr = '-'
            nameStr = '-'
            statusString = '-'
            stageString = '-'
        
        else:
            IDstr = f'{cmp.ID}'

            nameStr = f'"{cmp.getField("name", "-", verbose = False)}"'
            inds = cmp.indicatorsString()
            if inds is not None: nameStr += f' ({inds})'
        
            status = cmp.getField('status', '-', verbose = False)
            statusString = f'{status}'
        
            processStage = cmp.getField('processStage', '-', verbose = False)
            stageString = f'{processStage}'


        # string = f'({IDstr}) {nameStr:40} :: {statusString:30} :: {stageString:30}'
        string = f'{IDstr:28}{nameStr:35}{stageString:30}{statusString:30}'
        return string

    @staticmethod
    def _printComponentDashboardHeader():

        headerStr = f'{"Index":8}{"ID":28}{"Cmp. name":35}{"Process stage":30}{"Status":30}'
        print(headerStr)
        print(len(headerStr)*'-')

    def printModulesDashboard(self):

        print('Modules Dashboard')
        self._printComponentDashboardHeader()
        for index, mod in enumerate(self.modules):
            print(f'[{index:3}]   ' + self._cmpDashboardString(mod))

    def printCOSsDashboard(self):

        print('COSs Dashboard')
        self._printComponentDashboardHeader()
        for index, COS in enumerate(self.COSs):
            print(f'[{index:3}]   ' + self._cmpDashboardString(COS))

    def printChipsDashboard(self):

        print('Chips Dashboard')
        self._printComponentDashboardHeader()
        for index, chip in enumerate(self.chips):
            print(f'[{index:3}]   ' + self._cmpDashboardString(chip))


    def printDashboard(self,
                       printModules:bool = True,
                       printCOSs:bool = False,
                       printChips:bool = True):

        print(f'Module batch "{self.batch}" - Dashboard\n')

        if printModules: self.printModulesDashboard()
        if printCOSs: self.printCOSsDashboard()
        if printChips: self.printChipsDashboard()


    def _collectComponents(self, connection, batch, regexStrings):
        
        mods = self._queryModules(connection, batch, regexStrings)

        if mods is None:
            return None
        
        with mom.opened(connection):
            with mom.logMode(log, 'WARNING'):
                modCollations = [moduleCollation(connection, mod,
                                    collectBlueprints = False,
                                    verbose = False) for mod in mods]
        return mods, modCollations

    @staticmethod
    def _collectBlueprintsForGroup(connection, group:list,
                                   *,
                                   verbose:bool = True):
        """Given a group of components, this method queries the database and
        returns all the blueprints associated to them.

        Args:
            connection (mom.connection): The connection object to the MongoDB
                server.
            group (list[mom.component]): The list of components of which the
                blueprints have to be retrieved.

        Keyword Args:
            verbose (bool, optional): If False, query output is suppressed.
                Defaults to True.

        Returns:
            List[mom.blueprint] | None: The list of blueprints retrieved.
        """        

        # Selecting only component instances (None is excluded)
        group = [cmp for cmp in group if isinstance(cmp, mom.component)]
        bpIDs = [cmp.getField('blueprintID', verbose = False) for cmp in group]
        bpIDs = [mom.toObjectID(ID) for ID in bpIDs if ID is not None] # Removing None
        bpIDs = list(set(bpIDs)) # Removing duplicates

        bps = mom.query(connection, qu.among('_id', bpIDs), None,
                        mom.blueprint.defaultDatabase,
                        mom.blueprint.defaultCollection,
                        returnType = 'native', verbose = verbose)

        if bps is None:
            return None

        else:
            bps = [bp for bp in bps if bp is not None]
        
        if bps == []:
            return None
        
        return bps



    def _collectBlueprints(self, connection, *,
                           collectModuleBlueprints:bool = True,
                           collectCOSblueprints:bool = False,
                           collectChipBlueprints:bool = True,
                           verbose:bool = True,
                        ):
        """Collects the blueprints for modules, COSs and chips.
        It issues a warning when more than one blueprint is collected
        for each of the cathegories.

        Args:
            connection (mom.connection): The connection object to the MongoDB
                server.

        Keyword Args:
            collectModuleBlueprints (bool, optional): Whether to collect
                blueprints for modules. Defaults to True.
            collectCOSblueprints (bool, optional): Whether to collect
                blueprints for COSs. Defaults to False.
            collectChipBlueprints (bool, optional): Whether to collect
                blueprints for chips. Defaults to True.
        """        
        
        # Check module blueprints
        if not collectModuleBlueprints:
            modBPs = None

        else:
            if self.modules is None:
                if verbose: log.warning('No modules of which to collect blueprints.')
                modBPs = None

            modBPs = self._collectBlueprintsForGroup(connection, self.modules, verbose = verbose)
            if modBPs is None:
                if verbose: log.warning('No blueprint collected for modules.')
            else:
                if len(modBPs) > 1:
                    if verbose: log.warning('More than one blueprint collected for modules.')
                

        # Check COS blueprints
        if not collectCOSblueprints:
            COSbps = None
        else:

            if self.COSs is None:
                if verbose: log.warning('No COSs of which to collect blueprints.')
                COSbps = None

            COSbps = self._collectBlueprintsForGroup(connection, self.COSs, verbose = verbose)
            if COSbps is None:
                if verbose: log.warning('No blueprint collected for COSs.')
            else:
                if len(COSbps) > 1:
                    if verbose: log.warning('More than one blueprint collected for COSs.')


        # Check chip blueprints
        if not collectChipBlueprints:
            chipBPs = None
        else:
            if self.chips is None:
                if verbose: log.warning('No chips of which to collect blueprints.')
                chipBPs = None

            chipBPs = self._collectBlueprintsForGroup(connection, self.chips, verbose = verbose)
            if chipBPs is None:
                if verbose: log.warning('No blueprint collected for chips.')
            else:
                if len(chipBPs) > 1:
                    if verbose: log.warning('More than one blueprint collected for chips.')
        
        return modBPs, COSbps, chipBPs



    @staticmethod
    def _queryModules(connection, batch:str = None, regexStrings:list = None):
        """Queries the component's database for modules given their batch and
        a series of regex strings for their name."""

        if batch is None and regexStrings is None:
            raise TypeError(f'"batch" and "regexStrings" cannot be both None.')

        if regexStrings is not None:
            if not (isinstance(regexStrings, list) and 
                all([isinstance(s, str) for s in regexStrings])):
                raise TypeError('"regexStrings" must be a list of strings or None.')
        
        if batch is not None:
            if not isinstance(batch, str):
                raise TypeError('"batch" must be a string or None.')

        batchQuery = {'batch': batch} if batch is not None else None
        stringsQuery = qu.regex('name', regexStrings) if regexStrings is not None else None

        if batchQuery is None: query = stringsQuery
        elif stringsQuery is None: query = batchQuery
        else:
            query = qu.andPattern([batchQuery, stringsQuery])

        mods = mom.component.query(connection, query, verbose = False)

        if mods is None:
            log.warning('Could not retrieve modules.')
            return None
        
        log.spare(f'Query returned {len(mods)} modules.')
        return mods




