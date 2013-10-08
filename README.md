# Utility to automate requests and retriveal from the ESO data archive

Relevant parameters must be defined in **params.py**.  
  
**params.py** must be importable by EsoPortal.py

### Example usage:

    import EsoPortal  
    
    conn = EsoPortal.EsoPortal()  
    conn.login()  
    conn.queryArchive()  
    conn.createRequest()  
    conn.retrieveData()  
    conn.verifyData()  
    conn.logout()
