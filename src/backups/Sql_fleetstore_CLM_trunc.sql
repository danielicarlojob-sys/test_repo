/****** Script for SelectTopNRows command for CLIMB  ******/
SELECT 
        DA.[EngineSerialNumber] as ESN
        ,DA.[StartDatetime] as reportdatetime
        ,DA.[FirstReceivedDatetime] as datestored
        ,DA.[OperatorCode] as operator
        ,DA.[EngineId] as equipmentid
        ,DA.[AircraftIdentifier] as ACID
        ,DA.[EnginePosition] as ENGPOS
		,54 AS DSCID
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
        ,EPS.[TS25S__NOM_K]
        ,EPS.[PS30S__NOM_PSI]
        ,EPS.[TS30S__NOM_K]
        ,EPS.[TGTS__NOM_K]
        ,EPS.[NL__NOM_PC]
        ,EPS.[NI__NOM_PC]
        ,EPS.[NH__NOM_PC]
        ,EPS.[FF__NOM_LBHR]
        ,EPS.[P135S__NOM_PSI]
        ,DA.[ALT__FT]
        ,DA.[MN1]
        ,DA.[P20__PSI]
        ,DA.[T20__DEGC]
		
  FROM [A330Neo-Trent7000].[Climb-AircraftEngine-DA] AS DA
  JOIN [A330Neo-Trent7000].[Climb-AircraftEngine-EPS] AS EPS
  ON 
  DA.[EngineSerialNumber] = EPS.[EngineSerialNumber] AND
  DA.[StartDatetime] = EPS.[StartDatetime] AND
  DA.[OperatorCode] = EPS.[OperatorCode] AND
  DA.[EngineId] = EPS.[EngineId] AND
  DA.[AircraftIdentifier] = EPS.[AircraftIdentifier] AND
  DA.[EnginePosition] = EPS.[EnginePosition] 
  where DA.[StartDatetime] >= %startTimestamp% AND
  DA.[StartDatetime] <= '2025-06-01 00:00:00'
  ORDER BY DA.[StartDatetime]
