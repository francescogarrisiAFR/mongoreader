import mongoreader.wafers as morw

def PAM4_plotResult(waferName:str, resultName:str):

    if not isinstance(waferName, str):
        raise TypeError('"waferName" must be a string.')
    
    if not isinstance(resultName, str):
        raise TypeError('"resultName" must be a string.')

    def goggleIL(chip):

        try:
            hist = chip['processHistory']
            for entry in reversed(hist):
                results = entry['results']
                
                for res in results:
                    if 'IL' in res['resultName']:
                        data = res['resultData']
                        break

        except:
            return None

    w = morw.waferCollation_PAM4(waferName)

