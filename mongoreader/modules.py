import mongomanager as mom
from mongomanager import log, isID
from mongoutils import queryUtils as qu
import mongoreader.core as c
import mongoreader.errors as e


def queryModuleNames(conn, *strings, printNames:bool = True) -> list:
    """Queries and prints on console the components which conain "module" and
    any other "string" in their name (case insensitive) in their name.

    Suppress print on screen with printNames = False.
    """

    strings = ['module'] + [s for s in strings]

    query = qu.regex('name', strings, caseSensitive=False)

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
                            moduleRegexSearch)

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
                            moduleRegexSearch:bool = True):
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
                                            regex = moduleRegexSearch)
                
                self.COS = self._collectCOS(connection,
                                            self.module,
                                            COSvalidationCriterion)
                
                self.chip = self._collectChip(connection,
                                            self.COS,
                                            chipValidationCriterion)
                
                self.moduleBlueprint = self._collectModuleBlueprint(connection,
                                            self.module,
                                            moduleBlueprintValidationCriterion)
                
                self.COSblueprint = self._collectCOSblueprint(connection,
                                    self.COS,
                                    COSblueprintValidationCriterion)

                self.chipBlueprint = self._collectChipBlueprint(connection,
                                    self.chip,
                                    chipBlueprintValidationCriterion)

        mom.log.info(f'Collected documents.')
        

    @staticmethod
    def _collectModule(connection:mom.connection, module,
                      moduleValidationCriterion:callable = None,
                      regex:bool = True) -> mom.component:
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
            
            mod = mom.component.queryOne(connection, query, returnType='component')
            
            if mod is None:
                raise mom.DocumentNotFound(f'Could not query module "{module}".')

        else:
            raise TypeError(f'"module" is not a component, a string or an ID (it is {type(module)}).')
        
        if moduleValidationCriterion is not None:
            if not moduleValidationCriterion(mod):
                raise mom.DocumentNotFound(f'The collected module "{module.name}" did not pass the validation criterion.')
        
        mom.log.important(f'Collected module "{mod.name}".')
        return mod
    

    @staticmethod
    def _collectCOS(connection:mom.connection, module:mom.component,
                    COSvalidationCriterion:callable = None) -> mom.component:
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
        
        cmps = module.retrieveInnerComponents(connection)
        
        if cmps is None:
            mom.log.warning(f'Could not retrieve inner components from module "{module.name}".')
            return None
        
        COS = cmps[0]

        if COSvalidationCriterion is not None:
            if not COSvalidationCriterion(COS):
                mom.log.warning(f'The collected COS component "{COS.name}" did not pass the validation criterion.')
                return None
        
        mom.log.important(f'Collected COS "{COS.name}".')
        return COS


    @staticmethod
    def _collectChip(connection:mom.component, COS:mom.component,
                     chipValidationCriterion:callable = None) -> mom.component:
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

        cmps = COS.retrieveInnerComponents(connection)
        
        if cmps is None:
            mom.log.warning(f'Could not retrieve inner components from COS "{COS.name}".')
            return None
        
        chip = cmps[0]

        if chipValidationCriterion is not None:
            if not chipValidationCriterion(COS):
                mom.log.warning(f'The collected chip "{chip.name}" did not pass the validation criterion.')
                return None
        
        mom.log.important(f'Collected chip "{chip.name}".')
        return chip
    

    @staticmethod
    def _collectModuleBlueprint(connection:mom.connection, module:mom.component,
                            moduleBlueprintValidationCriterion:callable = None) -> mom.blueprint:
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
            bp = module.retrieveBlueprint(connection)
        except Exception:
            log.warning(f'Could not retrieve the blueprint associated to module "{module.name}".')
            return None
    
        if moduleBlueprintValidationCriterion is not None:
            if not moduleBlueprintValidationCriterion(bp):
                raise mom.DocumentNotFound(f'The collected module blueprint "{bp.name}" did not pass the validation criterion.')

        mom.log.important(f'Collected module blueprint "{bp.name}".')
        return bp
    

    @staticmethod
    def _collectCOSblueprint(connection:mom.connection, COS:mom.component,
                             COSblueprintValidationCriterion:callable = None) -> mom.blueprint:
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
            mom.log.warning('I cannot collect the COS blueprint because the COS was not collected.')
            return None

        try:
            bp = COS.retrieveBlueprint(connection)
        except Exception:
            log.warning(f'Could not retrieve the blueprint associated to COS "{COS.name}".')
            return None
        
        if bp is None:
            log.warning(f'Could not retrieve the blueprint associated to COS "{COS.name}".')
            return None
    
        if COSblueprintValidationCriterion is not None:
            if not COSblueprintValidationCriterion(bp):
                log.warning(f'The collected COS blueprint "{COS.name}" did not pass the validation criterion.')
                return None

        mom.log.important(f'Collected COS blueprint "{bp.name}".')
        return bp


    @staticmethod
    def _collectChipBlueprint(connection:mom.connection, chip:mom.component,
                             chipBlueprintValidationCriterion:callable = None) \
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
            mom.log.warning('I cannot collect the chip blueprint because the chip was not collected.')
            return None

        try:
            bp = chip.retrieveBlueprint(connection)
        except Exception:
            log.warning(f'Could not retrieve the blueprint associated to chip "{chip.name}".')
            return None
        
        if bp is None:
            log.warning(f'Could not retrieve the blueprint associated to chip "{chip.name}".')
            return None
    
        if chipBlueprintValidationCriterion is not None:
            if not chipBlueprintValidationCriterion(bp):
                log.warning(f'The collected chip blueprint "{bp.name}" did not pass the validation criterion.')
                return None

        mom.log.important(f'Collected chip blueprint "{bp.name}".')
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


