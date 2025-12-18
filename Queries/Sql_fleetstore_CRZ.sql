/****** Script for SelectTopNRows command for CRUISE  ******/
SELECT 
        DA.[EngineSerialNumber] as ESN
        ,DA.[StartDatetime] as reportdatetime
        ,DA.[FirstReceivedDatetime] as datestored
        ,DA.[OperatorCode] as operator
        ,DA.[EngineId] as equipmentid
        ,DA.[AircraftIdentifier] as ACID
        ,DA.[EnginePosition] as ENGPOS
		    ,52 AS DSCID
        ,DA.[P25__PSI]
        ,DA.[T25__DEGC]
        ,DA.[P30__PSI]
        ,DA.[T30__DEGC]
        ,DA.[TGTU_A__DEGC]
        ,DA.[NL__PC]
        ,DA.[NI__PC]
        ,DA.[NH__PC]
        ,DA.[FF__LBHR]
        ,DA.[PS160__PSI]
        ,EPS.[PS26S__NOM_PSI]
        ,EPS2.[TS25S__NOM_K]
        ,EPS.[PS30S__NOM_PSI]
        ,EPS2.[TS30S__NOM_K]
        ,EPS2.[TGTS__NOM_K]
        ,EPS.[NL__NOM_PC]
        ,EPS.[NI__NOM_PC]
        ,EPS.[NH__NOM_PC]
        ,EPS.[FF__NOM_LBHR]
        ,EPS.[P135S__NOM_PSI]
        ,DA.[ALT__FT]
        ,DA.[MN1]
        ,DA.[P20__PSI]
        ,DA.[T20__DEGC]

  FROM [A330Neo-Trent7000].[Cruise-AircraftEngine-DA] AS DA
  JOIN [A330Neo-Trent7000].[Cruise-AircraftEngine-EPS] AS EPS
  ON 
  DA.[EngineSerialNumber] = EPS.[EngineSerialNumber] AND
  DA.[StartDatetime] = EPS.[StartDatetime] AND
  DA.[OperatorCode] = EPS.[OperatorCode] AND
  DA.[EngineId] = EPS.[EngineId] AND
  DA.[AircraftIdentifier] = EPS.[AircraftIdentifier] AND
  DA.[EnginePosition] = EPS.[EnginePosition] 
  JOIN [A330Neo-Trent7000].[Cruise-AircraftEngine-EPS-2] AS EPS2

    ON 
  DA.[EngineSerialNumber] = EPS2.[EngineSerialNumber] AND
  DA.[StartDatetime] = EPS2.[StartDatetime] AND
  DA.[OperatorCode] = EPS2.[OperatorCode] AND
  DA.[EngineId] = EPS2.[EngineId] AND
  DA.[AircraftIdentifier] = EPS2.[AircraftIdentifier] AND
  DA.[EnginePosition] = EPS2.[EnginePosition] 
  where DA.deleted = 0 AND EPS.deleted = 0 AND EPS2.deleted = 0 AND DA.[StartDatetime] >= %startTimestamp%
  --- AND DA.[StartDatetime] < '2025-08-31 00:00:00'
  ORDER BY DA.[StartDatetime]
