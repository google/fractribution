This is a tutorial for how to run Fractribution on a sample GA360 dataset. We
recommend running through the tutorial on the sample dataset to verify that your
installation is working before tackling your own data.

The sample dataset comes from the Google Merchandise store, a real ecommerce
store (see
[here](https://support.google.com/analytics/answer/7586738?hl=en) for more
details). You can view the publicly-available, obfuscated data on BigQuery [here](https://bigquery.cloud.google.com/table/bigquery-public-data:google_analytics_sample.ga_sessions_20170801).

There are two stages to running Fractribution:

* Stage 1: Data Preparation in **py/fractribution_data**

  This stage generates the customer paths-to-conversion and
  paths-to-non-conversion via an end-to-end BigQuery and analytics pipeline.
  There are two ways to run this pipeline, depending on where the customer
  conversion data lives:

  *  Custom Endpoint: prepare_input_for_fractribution_custom_endpoint()

     The conversion data is already in the GA360 BigQuery table. In this
     tutorial we define conversion as a customer having a 'Contact Us' event.
     Because this data is already in GA360, we only need to supply a SQL WHERE
     filter to help extract the data. A sample filter is included in the file:
     ***py/templates/custom_endpoint_definition.sql***. This file also includes
     instructions on how to write your own filter more specific to your
     conversion event.

  *  Upload Endpoint: prepare_input_for_fractribution_upload_endpoint()

     If the conversion data is outside GA360, then you need to supply a BigQuery
     table via the flag ***endpoint_upload_bq_table*** that includes the fields:
     * customer_id: Your internal id for the customer.
     * endpoint_datetime: UTC timestamp of the endpoint in format
       'yyyy-mm-dd hh:mm:ss UTC'.

  In this tutorial, we will be using the Custom Endpoint. The main flags for
  this approach are:

  * ***project_id***: Google Cloud project to run Fractribution inside.
  * ***ga_sessions_table***: Name of the GA360 BigQuery table in the format
    \<PROJECT\>.\<DATASET\>.\<TABLE\>.
  * ***endpoint_definition***: File containing the custom endpoint definition,
    e.g. custom_endpoint_definition_example.sql.
  * ***channel_definitions***: File containing definitions and names of the
    marketing channels to extract in a path. The format is a CASE WHEN SQL
    statement to extract the data from GA360. See channel_definitions.sql for
    a default example.
  * ***report_window_start***: 'YYYY-mm-DD' date in UTC time to define the start
    of the reporting period (inclusive) to look for conversions.
  * ***report_window_end***: 'YYYY-mm-DD' date in UTC time to define the end
    of the reporting period (inclusive) to look for a conversion.
  * ***lookback_days***: An integer number of days to look back to build the
    paths to conversion. Recommended values include 30, 14 and 7. Note that
    a path contain marketing events before the report_window_start.
  * ***lookback_steps***: Optional restriction on the maximum number of
    marketing events in a path to conversion. This is used in conjunction with
    lookback_days. Recommendated values include 0 (in which case there is no
    restriction), and 5.
  * ***session_event_log_table***: Internal BigQuery table for storing
    intermediate user session data.
  * ***target_endpoints_table***: Internal BigQuery table for storing
    intermediate user conversion data.
  * ***paths_to_conversion_table***: Output BigQuery table for storing
    user-level paths that end in a conversion event.
  * ***paths_to_non_conversion_table***: Output BigQuery table for storing
    user-level paths that do not end in conversion.
  * ***path_summary_table***: Output BigQuery table for storing conversion and
    non-conversion oounts by path.
  * ***channel_counts_table***: Output BigQuery table for storing marketing
     events counts, aggregated by channel, campaign, source and medium.

* Stage 2: Model fitting in **py/fractribution_model**
  This stage runs a simplified Shapley Value DDA algorithm over the data
  prepared in Stage 1 to generate the fractional attribution values. Most of the
  flags have the same value as in Stage 1, since Stage 2 uses the output of
  Stage 1.

  * ***project_id***: Google Cloud project to run Fractribution inside.
  * ***report_window_start***: 'YYYY-mm-DD' date in UTC time to define the start
    of the reporting period (inclusive) to look for conversions.
  * ***report_window_end***: 'YYYY-mm-DD' date in UTC time to define the end
    of the reporting period (inclusive) to look for a conversion.
  * ***paths_to_conversion_table***: Output BigQuery table for storing
    user-level paths that end in a conversion event.
  * ***paths_to_non_conversion_table***: Output BigQuery table for storing
    user-level paths that do not end in conversion.
  * ***path_summary_table***: Output BigQuery table for storing conversion and
    non-conversion oounts by path.

  The following flags are new for Stage 2.

  * ***channels***: Comma separated list of channel names matching the
    channel_definitions. Note that one of the channels must be
    'Unmatched Channel'.
  * ***path_transform_method***: A path transform changes the
    path-to-conversions and path-to-non-conversions to reduce noise. There are
    4 transforms to choose from. Given a path of channels
    (D, A, B, B, C, D, C, C), the transforms are:

      * ***unique***: (identity transform): (D, A, B, B, C, D, C, C),
      * ***exposure***: (collapse sequential repeats, default option):
      (D, A, B, C, D, C),
      * ***first***: (remove repeats): (D, A, B, C),
      * ***frequency***: (remove repeats, but keep a count):
      (D(2), A(1), B(2), C(3))

  * ***report_table***: Output BigQuery table containing the use-level
    fractribution results. There is one row for each user, and one column for
    each channel.

## Deploying Fractribution on GCP: Cloud Functions vs VM

We recommend deploying Fractribution via Cloud Functions. Setup and maintenance
are easier, and because they are serverless, you only pay for what you use. The
main downsides are that Cloud Functions are limited to 2GB of RAM and 9 minutes
of runtime. If Cloud Functions run out of memory or time on your data, switch to
the VM approach, which allows you to select a compute engine with much higher
memory and no time limits.

Either way, for this tutorial, please select:

* ***\<PROJECT_ID\>***: GCP project in which to run Fractribution
* ***\<DATASET\>***: BigQuery dataset to store the Fractribution output. To
   create a dataset, either go through the
   [UI](https://cloud.google.com/bigquery/docs/datasets#classic-ui), or the
   [command line](https://cloud.google.com/bigquery/docs/datasets#bq)

## Running Python Cloud Functions

Running the Python Cloud Function version of Fractribution requires deploying
and running two cloud functions. The first function sets up the data tables,
and the second function runs the fractribution model on those tables.

### Setup

[Install gcloud SDK](https://cloud.google.com/sdk/install)

```
gcloud auth login && gcloud config set project <PROJECT_ID>
```

Download the fractribution folders to your local computer and change directory
into the fractribution_data folder.

### Deploying and Running the FractributionData Cloud Function

For this tutorial, we will create a Cloud Function called FractributionDataTest.

```
gcloud functions deploy FractributionDataTest \
--project <PROJECT_ID> \
--runtime python37 \
--entry-point prepare_input_for_fractribution_custom_endpoint \
--trigger-event google.pubsub.topic.publish \
--trigger-resource FractributionDataTestPubSub \
--timeout 540s \
--memory 2GB
```

Note that the entry-point, or main function, is
***prepare_input_for_fractribution_custom_endpoint***. Later, when you run on
your data, if the conversion data is not inside GA360, change the entry-point to
***prepare_input_for_fractribution_upload_endpoint***. Also, note that
FractributionDataTest is built from the code in the current directory. To start
with, this includes the sample channel and conversion definitions. When you
customize these later, you will have to create a new Cloud Function or overwrite
this one.

The trigger-* flags above setup how to run FractributionDataTest. The
trigger-resource flags creates a PubSub topic called
FractributionDataTestPubSub. The Cloud Function executes when it receives a
message on this topic. To publish a message and trigger FractributionDataTest,
use the following command:

```
gcloud pubsub topics publish FractributionDataTestPubSub --message '
{"project_id":"<PROJECT_ID>",
"ga_sessions_table":"bigquery-public-data.google_analytics_sample.ga_sessions_*",
"endpoint_definition":"custom_endpoint_definition_example.sql",
"channel_definitions":"channel_definitions.sql",
"report_window_start":"2016-08-01",
"report_window_end":"2017-08-01",
"lookback_days":"30",
"session_event_log_table":"<PROJECT_ID>.<DATASET>.session_event_log_table",
"target_endpoints_table":"<PROJECT_ID>.<DATASET>.target_endpoints_table",
"paths_to_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_conversion_table",
"paths_to_non_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_non_conversion_table",
"path_summary_table":"<PROJECT_ID>.<DATASET>.path_summary_table",
"channel_counts_table":"<PROJECT_ID>.<DATASET>.channel_counts_table"
}'
```

You can now go to your BigQuery \<DATASET\> to view the output of the first
stage of fractribution. We recommend looking at the channel_counts_table,
paths_to_conversion_table, and path_summary_table.

### Deploying and running the Model Cloud Function

Change directories on your machine to py/fractribution_model. Deploy the
FractributionModelTest with the following command:

```
gcloud functions deploy FractributionModelTest \
--project <PROJECT_ID> \
--runtime python37 \
--entry-point main \
--trigger-event google.pubsub.topic.publish \
--trigger-resource FractributionModelTestPubSub \
--timeout 540s \
--memory 2GB
```

To run FractributionModelTest, publish the arguments as a message to the PubSub
topic FractributionModelTestPubSub. Here is an example command:

```
gcloud pubsub topics publish FractributionModelTestPubSub --message '
{"project_id":"<PROJECT_ID>",
"report_window_start":"2016-08-01",
"report_window_end":"2017-08-01",
"paths_to_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_conversion_table",
"paths_to_non_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_non_conversion_table",
"path_summary_table":"<PROJECT_ID>.<DATASET>.path_summary_table",
"path_transform_method":"exposure",
"report_table":"<PROJECT_ID>.<DATASET>.report",
}'
```

Now you can go to your report_table and view the final user-level results
of fractribution.

Note that when you customize your channel definitions, you will need to pass
in the new channels as a comma-separated list via the channels flag discussed
earlier.


### Scheduling Fractribution to run end-to-end on the latest data.

There are two steps to accomplish here. First we need FractributionData to
trigger FractributionModel once it is finished. And secondly, we need to
automatically trigger FractributionData to run on the latest data according
to a given schedule.

#### Getting FractributionData to trigger FractributionModel:
If you pass model_topic_name as an argument to FractributionData, it will
publish a message to that topic when it is finished. The message includes all of
its arguments. So to get FractributionData to trigger FractributionModel, we
just need to include all of FractributionModel's arguments to the arguments for FractributionData, and also to pass the PubSub topic to trigger
FractributionModel.

#### Automatically triggering FractributionData on a given schedule:

1.  Decide how often you want to run Fractribution.

    CRON_SCHEDULE = Schedule on which Fractribution will be executed in
    cron-unix format.

    Example: 15 1 1 * * - Run every first day of the month at 1:15AM.

2.  Do not include report_window_start and report_window_end in the parameters,
    instead use these parameters to calculate the report window.

    CURRDATE_OFFSET = The number days offset from the current date.
    REPORT_WINDOW_LENGTH = The number of days in your report period.

    Example:

    -   If CURRDATE_OFFSET = 0 and REPORT_WINDOW_LENGTH = 7, report_window_end =
        Current Date and report_window_start = (report_window_end - 7 days)
    -   If CURRDATE_OFFSET = 2 and REPORT_WINDOW_LENGTH = 30,
        report_window_end = (Current Date - 2 day) and report_window_start =
        (report_window_end - 30 days)

3.  Trigger the FractributionModel after FractributionData is done by including
    the FractributionModel_TOPIC_NAME in the parameters.

4.  Create a cron job to run Fractribution using Cloud Scheduler.

    ```
    gcloud scheduler jobs create pubsub Fractribution --schedule
    "<CRON_SCHEDULE>" --topic FractributionData --message-body '{
    "project_id":"<PROJECT_ID>",
    "ga_sessions_table":"<CLIENT_GA_TABLE_PATH>.ga_sessions_*",
    "endpoint_definition":"custom_endpoint_definition_example.sql",
    "channel_definitions":"channel_definitions.sql",
    "lookback_days":"30",
    "lookback_steps":"0",
    "session_event_log_table":"<PROJECT_ID>.<DATASET>.session_event_log_table",
    "target_endpoints_table":"<PROJECT_ID>.<DATASET>.target_endpoints_table",
    "paths_to_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_conversion_table",
    "paths_to_non_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_non_conversion_table",
    "path_summary_table":"<PROJECT_ID>.<DATASET>.path_summary_table",
    "channel_counts_table":"<PROJECT_ID>.<DATASET>.channel_counts_table",
    "report_window_length":"<REPORT_WINDOW_LENGTH>",
    "report_window_end_offset_from_currdate":"<CURRDATE_OFFSET>",
    "model_topic_name":"<FractributionModel_TOPIC_NAME>",
    "path_transform_method":"exposure",
    "report_table":"<PROJECT_ID>.<DATASET>.report"}'
    ```

### Debugging Fractribution

If you are planning to debug, [install python](https://cloud.google.com/python/setup?hl=en#installing_python)
and optionally use a virtual environment to sandbox your dependencies.

This example uses a Python virtual environment. The following commands are
creating a virtual environment, installing the dependencies from
requirements.txt, and then using functions-framework to deploy your cloud
function on your local machine.

```
$virtualenv fracenv
$source fracenv/bin/activate
$cd <your fractribution_data dir>
$pip3 install -r requirements.txt
```

#### Debugging the Data Cloud Function

```
$functions-framework --target prepare_input_for_fractribution_custom_endpoint \
--signature-type=event --debug
```

The way to call the local version of your cloud version is slightly different to
how you would normally do it using a PubSub message. Your local cloud function
is running within a web server, and you have to manually encode the parameters
before you call the web version. To base64 encode the parameters including the
surround braces {}, you can use one of the commands below depending on your
operating system. We recommend you do not use third-party websites to encode
your parameters as you may be leaking sensitive information.

Linux:
```
echo -n '{"p1":”v1”, “p2”:”v2”}' | base64
```

Mac:
```
echo -n '{"p1":”v1”, “p2”:”v2”}' | openssl base64
```

Windows PowerShell:
```
[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(‘{"p1”:”val1”, ”p2”:val2”}’))
```

Now you can copy that encoded text into the command below to call the local web
server containing your cloud function:

```
curl -d '{"data": {"data": "<PUT_ENCODED_PARAMS_HERE>"}}' \-X POST -H "Content-Type: application/json" http://0.0.0.0:8080
```

That should capture both print statements and debug traces when things go wrong.

#### Debugging the Model Cloud Function

In your python virtual environment with requirements.txt dependencies installed:
```
functions-framework --target main --signature-type=event --debug
```
```
curl -d '{"data": {"data": "<BASE_64_ENCODED_PARAMS"}}' \-X POST -H "Content-Type: application/json" http://0.0.0.0:8080
```


## Deploy Fractribution Docker Image on GCP VM Instance

1.  Variables:

    1.  \<Fractribution_Region\> - Region of the VM Instance
    2.  \<Fractribution_Zone\> - Zone of the VM Instance
    3.  \<Fractribution_Service_Account\> - Service Account for Fractribution.
        It should have the following roles:
        *  BigQuery Data Editor
        *  BigQuery Job User
        *  Compute Instance Admin (beta)
        *  Logs Writer
        *  Storage Object Viewer
    4.  \<Fractribution_Schedule\> - Schedule on which Fractribution will be
        executed in cron-unix format. Example: “15 1 1 * *” - Run every first
        day of the month at 1:15AM.

    5.  \<Fractribution_Param\> - Fractribution parameters for data and model in
        JSON format. Example value below:

        ```
        '{"project_id":"<PROJECT_ID>",
        "ga_sessions_table":"<CLIENT_GA_TABLE_PATH>.ga_sessions_*",
        "endpoint_definition":"custom_endpoint_definition_example.sql",
        "channel_definitions":"channel_definitions.sql",
        "lookback_days":"30",
        "lookback_steps":"0",
        "session_event_log_table":"<PROJECT_ID>.<DATASET>.session_event_log_table",
        "target_endpoints_table":"<PROJECT_ID>.<DATASET>.target_endpoints_table",
        "paths_to_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_conversion_table",
        "paths_to_non_conversion_table":"<PROJECT_ID>.<DATASET>.paths_to_non_conversion_table",
        "path_summary_table":"<PROJECT_ID>.<DATASET>.path_summary_table",
        "channel_counts_table":"<PROJECT_ID>.<DATASET>.channel_counts_table",
        "report_window_length":"<REPORT_WINDOW_LENGTH>",
        "report_window_end_offset_from_currdate":"<CURRDATE_OFFSET>",
        "model_topic_name":"<FractributionModel_TOPIC_NAME>",
        "path_transform_method":"exposure",
        "report_table":"<PROJECT_ID>.<DATASET>.report"}'
        ```

2.  Create a docker image of the Fractribution.

    ```bash
    gcloud builds submit --tag gcr.io/\<project_id\>/\<image-name\>
    ```

3.  Set up the Compute Engine Instance

    1.  Go to VM instances page
        https://console.cloud.google.com/compute/instances

    2.  Click Create instance.

    3.  Set the Name.

    4.  Click Add label. Enter env for Key and fractribution for Value.

    5.  Select Region select \<Fractribution_Region\>.en

    6.  For Zone select \<Fractribution_Zone\>.

    7.  Select Deploy a container image.

    8.  Specify the container image created in Step #1.

    9.  Expand Advanced container options section.

    10. Under Environment variables, click Add variable. Enter
        fractribution_param for NAME and \<Fractribution_Param\> for VALUE.

    11. Under Identity and API access, for Service account, select
        \<Fractribution_Service_Account\>.

    12. Click Create at the bottom of the page.

4.  Set up Cloud Function to start a VM instance. (Reference)

    1.  Go to the Cloud Functions page in the Cloud Console.

    2.  Click Create Function.

    3.  Set the Name to startInstancePubSub.

    4.  Leave Memory allocated at its default value.

    5.  For Trigger, select Cloud Pub/Sub.

    6.  For Topic, select Create new topic....

    7.  A New pub/sub topic dialog box should appear.

    8.  Under Name, enter start-instance-event.

    9.  Click Create to finish the dialog box.

    10. For Runtime, select Node.js 10.

    11. Above the code text block, select the index.js tab.

    12. Replace the starter code with the following code:

    ```
    const Compute = require('@google-cloud/compute');
    const compute = new Compute();
    /**
     * Starts Compute Engine instances.
     *
     * Expects a PubSub message with JSON-formatted event data containing the
     * following attributes:
     *  zone - the GCP zone the instances are located in.
     *  label - the label of instances to start.
     *
     * @param {!object} event Cloud Function PubSub message event.
     * @param {!object} callback Cloud Function PubSub callback indicating
     *  completion.
     */
    exports.startInstancePubSub = async (event, context, callback) => {
      try {
        const payload = _validatePayload(
          JSON.parse(Buffer.from(event.data, 'base64').toString())
        );
        const options = {filter: `labels.${payload.label}`};
        const [vms] = await compute.getVMs(options);
        await Promise.all(
          vms.map(async (instance) => {
            if (payload.zone === instance.zone.id) {
              const [operation] = await compute
                .zone(payload.zone)
                .vm(instance.name)
                .start();

              // Operation pending
              return operation.promise();
            }
          })
        );

        // Operation complete. Instance successfully started.
        const message = `Successfully started instance(s)`;
        console.log(message);
        callback(null, message);
      } catch (err) {
        console.log(err);
        callback(err);
      }
    };

    /**
     * Validates that a request payload contains the expected fields.
     *
     * @param {!object} payload the request payload to validate.
     * @return {!object} the payload object.
     */
    const _validatePayload = (payload) => {
      if (!payload.zone) {
        throw new Error(`Attribute 'zone' missing from payload`);
      } else if (!payload.label) {
        throw new Error(`Attribute 'label' missing from payload`);
      }
      return payload;
    };
    ```

    1.  Above the code text block, select the package.json tab.

    2.  Replace the starter code with the following code:

    ```
    {
      "name": "cloud-functions-schedule-instance",
      "version": "0.1.0",
      "private": true,
      "license": "Apache-2.0",
      "author": "Google LLC",
      "repository": {
        "type": "git",
        "url": "https://github.com/GoogleCloudPlatform/nodejs-docs-samples.git"
      },
      "engines": {
        "node": ">=8.0.0"
      },
      "scripts": {
        "test": "mocha test/*.test.js --timeout=20000"
      },
      "devDependencies": {
        "mocha": "^7.0.0",
        "proxyquire": "^2.0.0",
        "sinon": "^9.0.0"
      },
      "dependencies": {
        "@google-cloud/compute": "^1.0.0"
      }
    }
    ```

    1.  For Function to execute, enter startInstancePubSub.

    2.  Click Create.

5.  Set up Cloud Scheduler to trigger Pub/Sub. (Reference)

    1.  Go to the Cloud Scheduler page in the Cloud Console.

    2.  Click Create Job.

    3.  Set the Name to startup-fractribution-instance.

    4.  For Frequency, enter \<Fractribution_Schedule\>.

    5.  For Timezone, select your desired country and timezone.

    6.  For Target, select Pub/Sub.

    7.  For Topic, enter start-instance-event.

    8.  For Payload, enter the following:
        {"zone":"\<Fractribution_Zone\>","label":"env=fractribution"}

    9.  Click Create.
