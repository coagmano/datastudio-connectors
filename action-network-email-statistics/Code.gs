var cc = DataStudioApp.createCommunityConnector();

// https://developers.google.com/datastudio/connector/reference#getauthtype
function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.KEY)
    .setHelpUrl("https://actionnetwork.org/apis")
    .build();
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty("dscc.key");
}

function checkForValidKey(key) {
  const response = UrlFetchApp.fetch(
    "https://actionnetwork.org/api/v2/messages",
    {
      headers: {
        "OSDI-API-Token": key,
      },
    }
  );

  return response.getResponseCode() === 200;
}
/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty("dscc.key");
  return checkForValidKey(key);
}

/**
 * Sets the credentials.
 * @param {Request} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var key = request.key;

  // Optional
  // Check if the provided key is valid through a call to your service.
  // You would have to have a `checkForValidKey` function defined for
  // this to work.
  var validKey = checkForValidKey(key);
  if (!validKey) {
    return {
      errorCode: "INVALID_CREDENTIALS",
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty("dscc.key", key);
  return {
    errorCode: "NONE",
  };
}

// https://developers.google.com/datastudio/connector/reference#getconfig
/*
function getConfig() {
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText(
      'Enter npm package names to fetch their download count. An invalid or blank entry will revert to the default value.'
    );

  config
    .newTextInput()
    .setId('package')
    .setName(
      'Enter a single package name or multiple names separated by commas (no spaces!)'
    )
    .setHelpText('e.g. "googleapis" or "package,somepackage,anotherpackage"')
    .setPlaceholder(DEFAULT_PACKAGE)
    .setAllowOverride(true);

  config.setDateRangeRequired(true);

  return config.build();
}
*/

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  // Basic fields
  // Dates arrive in this format "2020-04-30T23:22:06Z"
  fields
    .newMetric()
    .setId("created_date")
    .setName("Created Date")
    .setType(types.YEAR_MONTH_DAY_SECOND);
  fields
    .newMetric()
    .setId("modified_date")
    .setName("Modified Date")
    .setType(types.YEAR_MONTH_DAY_SECOND);
  fields.newMetric().setId("subject").setName("Subject").setType(types.TEXT);
  fields.newMetric().setId("from").setName("From").setType(types.TEXT);
  fields.newMetric().setId("reply_to").setName("Reply To").setType(types.TEXT);
  fields
    .newMetric()
    .setId("administrative_url")
    .setName("Administrative Url")
    .setType(types.HYPERLINK);
  fields
    .newMetric()
    .setId("total_targeted")
    .setName("Total Targeted")
    .setType(types.NUMBER);
  fields.newMetric().setId("status").setName("Status").setType(types.TEXT);
  fields
    .newMetric()
    .setId("sent_start_date")
    .setName("Sent Start Date")
    .setType(types.YEAR_MONTH_DAY_SECOND);
  fields.newMetric().setId("type").setName("Type").setType(types.TEXT);
  // Statistics
  fields.newMetric().setId("sent").setName("Sent").setType(types.NUMBER);
  fields.newMetric().setId("opened").setName("Opened").setType(types.NUMBER);
  fields.newMetric().setId("clicked").setName("Clicked").setType(types.NUMBER);
  fields.newMetric().setId("actions").setName("Actions").setType(types.NUMBER);
  fields
    .newMetric()
    .setId("unsubscribed")
    .setName("Unsubscribed")
    .setType(types.NUMBER);
  fields.newMetric().setId("bounced").setName("Bounced").setType(types.NUMBER);
  fields
    .newMetric()
    .setId("spam_reports")
    .setName("Spam Reports")
    .setType(types.NUMBER);
  // Formulae
  fields
    .newMetric()
    .setId("open_rate")
    .setName("Open Rate")
    .setType(types.PERCENT)
    .setFormula("$opened / $sent");
  fields
    .newMetric()
    .setId("click_rate")
    .setName("Click Rate")
    .setType(types.PERCENT)
    .setFormula("$clicked / $sent");
  fields
    .newMetric()
    .setId("action_rate")
    .setName("Action Rate")
    .setType(types.PERCENT)
    .setFormula("$actions / $sent");
  fields
    .newMetric()
    .setId("unsubscribe_rate")
    .setName("Unsubscribe Rate")
    .setType(types.PERCENT)
    .setFormula("$unsubscribed / $sent");
  fields
    .newMetric()
    .setId("bounce_rate")
    .setName("Bounce Rate")
    .setType(types.PERCENT)
    .setFormula("$bounced / $sent");
  fields
    .newMetric()
    .setId("spam_rate")
    .setName("Spam Rate")
    .setType(types.PERCENT)
    .setFormula("$spam_reports / $sent");

  return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
  return { schema: getFields().build() };
}

// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
  // request.configParams = validateConfig(request.configParams);

  var requestedFields = getFields().forIds(
    request.fields.map(function (field) {
      return field.name;
    })
  );

  try {
    var apiKey = userProperties.getProperty("dscc.key");
    var apiResponse = fetchDataFromApi(request, apiKey);
    var normalizedResponse = normalizeResponse(request, apiResponse);
    var data = getFormattedData(normalizedResponse, requestedFields);
  } catch (e) {
    cc.newUserError()
      .setDebugText("Error fetching data from API. Exception details: " + e)
      .setText(
        "The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists."
      )
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: data,
  };
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function fetchDataFromApi(request, apiKey) {
  var url = "https://actionnetwork.org/api/v2/messages/";
  var response = UrlFetchApp.fetch(url, {
    headers: {
      "OSDI-API-Token": apiKey,
    },
  });
  return response;
}

/**
 * Parses response string into an object. Also standardizes the object structure
 * for single vs multiple packages.
 *
 * @param {Object} request Data request parameters.
 * @param {string} responseString Response from the API.
 * @return {Object} Contains package names as keys and associated download count
 *     information(object) as values.
 */
function normalizeResponse(request, responseString) {
  return JSON.parse(responseString);
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, requestedFields) {
  const fieldIds = requestedFields.asArray().map((f) => f.getId());
  var data = response["_embedded"]["osdi:messages"];
  return data
    .filter(({ status }) => status === "sent")
    .map((doc) => {
      const newDoc = {};
      requestedFields.forEach((f) => (newDoc[f] = doc[f]));
      return {
        ...newDoc,
        ...doc.statistics,
      };
    });
}
