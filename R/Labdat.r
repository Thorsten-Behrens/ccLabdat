
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#' Extract and combine raw data from ccCleanLabLoadDirect
#'
#' Load various datasets from db based on project names
#'
#' @param vsIdSampeKobo vsIdSampeKobo Field codes to extract; vector of field codes. Names must be the same as in CleanLab column "field_code" (e.g. "PREZ 123")
#' @param sDbCleanLabVersion Version of DbCleanLab to use;  Default: "DbCleanLab". "DbCleanLabDevelopment" should be used for testing during development
#' @param sConfigFile Path to the config file (.yml) with the database credentials
#'
#' @return list of dataframes
#'
#' @author Daniel Wächter, \email{daniel.waechter@@bfh.ch}
#'
#' @export
#'
ccCleanLabLoadDirect = function(vsFieldCodes = "",
                                sDbCleanLabVersion = "DbCleanLab",
                                sConfigFile = "config.yml")
{

  ## For testing only (delete later)
  #  vsIdSampeKobo = "PREZ 123"
  #  sDbCleanLabVersion = ""
  #  sConfigFile = "C:/Users/waech/OneDrive - Berner Fachhochschule/999_R/cc/ccKeys/config.yml"

  lDfCleanLabData = list()

  ## Prepare empty lists
  lDfCleanLabData[["dfLabMeasurements"]] = data.frame()
  lDfCleanLabData[["dfMethods"]] = data.frame()
  lDfCleanLabData[["TechnicalInformations"]] = data.frame()

  ## Load all required packages
  require(DBI, quietly = TRUE)
  require(RPostgreSQL, quietly = TRUE)
  require(stringi, quietly = TRUE)
  require(RPostgres, quietly = TRUE)
  require(hms, quietly = TRUE)

  ## Setting parameters (start time, version, ...) ####
  ## Set Start time
  dTimeStart = Sys.time()

  ## Version of the function
  nVersion = 0.1

  ## Any text to the version
  sVersionNote = "DbCleanLab separared"

  ## check for missing projects
  if (length(vsFieldCodes) == 0)
  {
    return(NULL)
    warning("No field codes provided. Exiting...")

  }

  ## Collect technical information and make dfVersion
  dfVersion = data.frame(CodePart = "ccCleanLabLoad",
                         CodeVersion = nVersion,
                         CodeNote = sVersionNote,
                         User = Sys.info()[["user"]],
                         RVersion = R.Version()$version.string,
                         timespamp = format(Sys.time(),'%Y_%m_%d %H:%M:%S'))

  ## prepare database connections soildat and cleanlab
  lDbConCleanLab = list()

  ## Get the database credentials
  lDbConCleanLab = edDbCredsYaml(sDataBaseName = "DbCleanLab", sUser = "UsrLabRead", sConfigFile = sConfigFile)

  ## Get the database credentials
  ### Measurements ####

  ## Prepare sql query
  sqlQuery = ccQueryMeasurement(vsFieldCodes)

  ## Get data from server
  connCleanLab = ccDbConnect(lDbConCleanLab)
  dfLabMeasurements = dbGetQuery(connCleanLab, sqlQuery)

  # Disconnect from the clean lab database
  dbDisconnect(connCleanLab)

  ## Check if dfSamplesMeas is not empty
  if(nrow(dfLabMeasurements) != 0)
  {
    # Check if there are any measurements
    if (nrow(dfLabMeasurements) != 0)
    {
      # What do you think is the sample ID?
      # dfSamples$IdSampleKobo
      dfLabMeasurements$IdSampleKoboBackUpFromMeasTab = dfLabMeasurements$id


      # pH treatment: We need to add the extraction to pH to separate pH H2O from pH CaCl2
      if (nrow(dfLabMeasurements[which(dfLabMeasurements$nabo_analysis_parameter_code == "pH"), ]) != 0)
      {
        dfLabMeasurements[which(dfLabMeasurements$nabo_analysis_parameter_code == "pH"), ]$nabo_analysis_parameter_code =
          paste0(
            dfLabMeasurements[which(dfLabMeasurements$nabo_analysis_parameter_code == "pH"), ]$nabo_analysis_parameter_code,
            "(",
            dfLabMeasurements[which(dfLabMeasurements$nabo_analysis_parameter_code == "pH"), ]$nabo_extraction_method_code,
            ")"
          )
      }

      ### Extract methods #######
      # Select columns for "header"
      vsSelectedColumnsMethode = c(
        "analysis_group_code", "analysis_group_description",
        "analysis_parameter_code", "analysis_parameter_description",
        "unit_code",
        "preparation_method_code", "preparation_method_description",
        "extraction_method_code", "extraction_method_description",
        "measurement_method_code", "measurement_method_description",
        "id_analysis_parameter", "id_extraction_method", "id_laboratory",
        "id_preparation_method", "id_unit")
      # options(str = strOptions(list.len = 200))
      # str(dfLabMeasurements)

      ## Print missing vsSclectedColumnsMethode in headers dfLabMeasurements
      # print(setdiff(vsSelectedColumnsMethode, colnames(dfLabMeasurements)))

      # Subset dfLabMeasurements by selected columns
      dfMethods = unique(subset(dfLabMeasurements, select = vsSelectedColumnsMethode))
    }

    ## Technical updates ####
    ## Set End Time
    dTimeEnd  = Sys.time()
    ## Set time diff
    sTimeDiff = difftime(dTimeEnd, dTimeStart)

    # Get hh:mm:ss from time diff
    sTimeDiff = as_hms(sTimeDiff)

    # Round time diff
    dfVersion$TimeTaken = sTimeDiff


    ## Store data in list
    lDfCleanLabData[["dfLabMeasurements"]] = dfLabMeasurements
    lDfCleanLabData[["dfMethods"]] = dfMethods
    lDfCleanLabData[["TechnicalInformations"]] = dfVersion
  }

  ## Return list
  return(lDfCleanLabData)
}


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#' TryCatch function for ccCleanLabLoadDirect
#'
#' @param vsIdSampeKobo vsIdSampeKobo Field codes to extract; vector of field codes. Names must be the same as in CleanLab column "field_code" (e.g. "PREZ 123")
#' @param sDbCleanLabVersion Version of DbCleanLab to use;  Default: "DbCleanLab". "DbCleanLabDevelopment" should be used for testing during development
#' @param sConfigFile Path to the config file (.yml) with the database credentials
#'
#' @param nDelay Delay in seconds between each try
#' @param nTimes Number of times to repeat (tries)
#' @param sConfigFile Path to the config file (.yml) with the database credentials
#'
#' @return list of dataframes
#'
#' @author Daniel Wächter, \email{daniel.waechter@@bfh.ch}, Thorsten Behrens, \email{thorsten.behrens@@bfh.ch} and Marie Hertzog \email{marie.hertzog@@bfh.ch}
#'
#' @export
#'
ccCleanLabLoad = function(vsFieldCodes = "",
                          sDbCleanLabVersion = "DbCleanLab",
                          sConfigFile = "//bfh.ch/data/LFE/HAFL/KOBO/998_KOBO_Data_and_Apps/01_DataManagement/00_Functions_and_Packages/cc/ccKeys/config_kobo.yml",
                          nDelay = 20, # Seconds
                          nTimes = 6)

{
  ## TryCatch ccDbSoildatLoadDirect while error repeat for nTimes with a deley of nDelay
  Feedback = "Error in ccCleanLabLoadDirect"
  nCounter = 1
  while(Feedback == "Error in ccCleanLabLoadDirect" & nCounter <= nTimes){

    ## Message to user
    cat(paste0("Try ", nCounter, " of ", nTimes))

    ## Update Counter
    nCounter = nCounter + 1

    tryCatch({
      cat(": Try to extract data from CleanLab")
      lLabData = ccCleanLabLoadDirect(vsFieldCodes = vsFieldCodes,
                                      sDbCleanLabVersion = sDbCleanLabVersion,
                                      sConfigFile = sConfigFile)
      Feedback = "Success"
      cat(" -> Success \n")
      return(lLabData)

    }, error = function(e) {
      Feedback = "Error in ccCleanLabLoadDirect"
      cat(paste0(" -> Error in ccCleanLabLoadDirect, redo with a delay of ", nDelay, " seconds\n"))
      Sys.sleep(nDelay)
      # nCounter <- nCounter + 1
    })
  }




}

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#' Builds a postgres query for selecting soil layer by observation id from Soildat
#'
#' @param sObsIdsQuery String of soildat observation ids
#'
#' @return string with postgres query for soildat
#'
#'
#' @author Daniel Wächter, \email{daniel.waechter@@bfh.ch} and Thorsten Behrens, \email{thorsten.behrens@@bfh.ch}
#'
#' @export
#'
ccQueryMeasurement = function(vsFieldCodes)
{
  ## Collapse to sting
  ## For testing only
  # vsFieldCodes = c("PREZ_123", "PREZ_124")


  ## Collapse vsSampleIds to string every element needs "" and the whole string needs ''
  sIds = paste0("'", paste(vsFieldCodes, collapse = "', '"), "'")

  ## Build query
  sQuery = paste0('SELECT

	sample.id as id_sample_soildat,
	sample.depth_to as depth_to,
	sample.depth_from as depth_from,
	sample.code as field_code,
	sample.comments as sample_comments,
	sample."IdSampleKobo" as id_sample_kobo,

	measurement."value" as value_num,
	measurement.lab_date as lab_date,
	measurement.limit_of_determination as limit_of_determination_num,

	nabo_analysis_group.code as analysis_group_code,
	nabo_analysis_group.description_de as analysis_group_description,
	nabo_analysis_parameter.code as analysis_parameter_code,
	nabo_analysis_parameter.id as id_analysis_parameter,
	nabo_analysis_parameter.description_de as analysis_parameter_description,
	nabo_extraction_method.code as extraction_method_code,
	nabo_extraction_method.id as id_extraction_method,
	nabo_extraction_method.description_de as extraction_method_description,
	nabo_measurement_method.code as measurement_method_code,
	nabo_measurement_method.description_de as measurement_method_description,
	nabo_preparation_method.code as preparation_method_code,
	nabo_preparation_method.id as id_preparation_method,
	nabo_preparation_method.description_de as preparation_method_description,
	nabo_unit.code as unit_code,
	nabo_unit.id as id_unit,
	soildat04_laboratory.code as laboratory_code,
	soildat04_laboratory.id as id_laboratory

FROM measurement
    Left JOIN sample ON measurement.sample_id = sample.id
	LEFT JOIN nabo_analysis_parameter on nabo_analysis_parameter.id = measurement.analysis_parameter_id
	LEFT JOIN nabo_extraction_method  on nabo_extraction_method.id = measurement.extraction_method_id
	LEFT JOIN nabo_measurement_method on nabo_measurement_method.id = measurement.method_id
	LEFT JOIN nabo_preparation_method on nabo_preparation_method.id = measurement.preparation_method_id
	LEFT JOIN nabo_unit on nabo_unit.id = measurement.unit_id
	LEFT JOIN nabo_analysis_group on nabo_analysis_group.id = measurement.analysis_group_id
	LEFT JOIN soildat04_laboratory on soildat04_laboratory.id = measurement.laboratory_id',
                  ' WHERE sample.code in (', sIds, ');'
  )

  # Return query
  return(sQuery)

}

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#' Extract, combine and clean data from Soildat
#'
#' Load various datasets from Soildat based on project names
#'
#' @param lDicts List of KOBO naming and convention dictionaries
#' @param lDbCon List of database credentials generated with Creds
#' @param sDbSoildat Name of the database containing Soildat data
#' @param sDbCleanlab Name of the atabase containing clean lab data
#' @param sProject Project to extract; only one project can be extracted
#' @param sUseCase Specific use case as defined in the main name encoding dict
#'
#' @return list of dataframes
#'
#' @author Daniel Wächter, \email{daniel.waechter@@bfh.ch}, Thorsten Behrens, \email{thorsten.behrens@@bfh.ch} and Marie Hertzog \email{marie.hertzog@@bfh.ch}
#'
#' @export
#'
ccCleanLabTransform = function(lCleanLab,
                               sUseCase = "CaseDebug",
                               sFnDictsXls,
                               sConfigFile = "config.yml"
                               # sSoildatVersion = "Kobo",
                               # bMakeColumnsShort = FALSE
)
{
  ### For Testing only
  # sProject = "PREZ_Z1_E1"
  # sUseCase = "CaseModelling"
  # sFnDictsXls = paste0(sPathcc,"/ccSoildat/data/SoildatDictionaries.xlsx")
  # sConfigFile = "C:/Users/waech/OneDrive - Berner Fachhochschule/999_R/cc/ccKeys/config.yml"
  # lCleanLab =  lCleanLabNew

  ## Prepare empty dfs

  # Setup ####
  # Set version and note
  nVersion = 0.1
  sVersionNote = "There's a fly in the ointment."

  # Set Start time
  dTimeStart = Sys.time()

  # Load required libraries
  require("doBy")
  require("matchmaker")
  require("stringr")

  # extract technical Informations
  dfTechnicalInfo = lCleanLab$TechnicalInformations

  # Collect technical informations
  dfVersion = data.frame(CodePart = "ccCleanLabTransform",
                         CodeVersion = nVersion,
                         CodeNote = sVersionNote,
                         User = Sys.info()[["user"]],
                         RVersion = R.Version()$version.string,
                         timespamp = format(Sys.time(),'%Y_%m_%d %H:%M:%S'))

  # Lists needed from SoildatRaw
  vsListRequired = c("dfLabMeasurements", "dfMethods", "TechnicalInformations")

  # Set diff of required and existing
  vsDismatch = setdiff(vsListRequired, names(lCleanLab))

  # If there is a diff, stop and show the missing lists
  if(length(vsDismatch) != 0)
  {
    stop(paste0("List(s) from SoildatRaw are missing. Set all parameters as TRUE. \n Check: ", to_str(vsDismatch, collapse = ", ")))
  }

  ## lDicts #####
  # Load lDicts
  lDicts = ccSoildatReadXlsDict(sFnDictsXls, sConfigFile, sSoildatVersion, bUseDevelopmentViews)

  # Prepare empty dfs
  dfMeasurements = data.frame()
  dfNameMeasurements = data.frame()
  dfMeasurementsWide = data.frame()

  # Get Lab measurements ####
  # Get measurements if existing

  # Get measurements for list
  dfMeasurements = lCleanLab$dfLabMeasurements

  # Load useCase selection
  dfNameEncoding = lDicts$measurements

  # Select useCase  = TRUE
  dfNameMeasurements = dfNameEncoding[dfNameEncoding[sUseCase] == TRUE, ]

  # Check if nrow(df) != 0 AND measurements are requested
  if (nrow(dfMeasurements) != 0 & nrow(dfNameMeasurements) != 0 & ncol(dfMeasurements) > 1)
  {
    ## Apply ccMeasurement to get wide format
    dfMeasurementsWide = ccMeasurement(dfMeasurements, lDicts, sUseCase)

    ## Get Methodes from list
    dfMethods = lCleanLab$dfMethods

    ## Only take methodes that are in dfMeasurementsWide
    dfNameMeasurements = subset(dfNameMeasurements, dfNameMeasurements$code %in% names(dfMeasurementsWide))
    dfMethods2 = base::merge(subset(dfNameMeasurements, select = c("Messwert", "analyte", "code")), dfMethods, by.x = "Messwert" , by.y = "analysis_parameter_code")
  }

  lCleanLab[["dfLabMeasurementsWide"]] = unique(dfMeasurementsWide)
  lCleanLab[["dfMethods"]]            = unique(dfMethods2)

  ## Technical updates ####
  ## Set End Time
  dTimeEnd  = Sys.time()
  ## Set time diff
  sTimeDiff = difftime(dTimeEnd, dTimeStart)

  # Get hh:mm:ss from time diff
  sTimeDiff = as_hms(sTimeDiff)

  # Round time diff
  dfVersion$TimeTaken = sTimeDiff

  # Add new technical information to df
  dfTechnicalInfo = rbind(dfTechnicalInfo, dfVersion)

  lCleanLab[["TechnicalInformations"]]   = dfTechnicalInfo

  # return selected Soildat data
  return(lCleanLab)
}

