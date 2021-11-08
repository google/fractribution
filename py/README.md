## Setting up Fractribution:

### Step 1: Extracting customer conversions:

By default, `templates/extract_conversions.sql` runs over the GA360 BigQuery
table using the conversion definition in `templates/conversion_definition.sql`.
In most cases, you should only have to edit
`templates/conversion_definition.sql`. However, if your conversions are stored
outside the GA360 BigQuery table, use the instructions in
`templates/extract_conversions.sql` to replace it with a custom script.

The conversion window is the period of time that Fractribution will look to
extract conversions. The window is specified by passing in the following flags:

* ***`conversion_window_end_date`***: `'YYYY-MM-DD'` date in UTC time.
    Conversions up to midnight on this date are included.
* ***`conversion_window_end_today_offset_days`***: Sets the
    `conversion_window_end_date` as an offset from today's date. This is an
    alternative to `conversion_window_end_date` used in regular scheduled runs
    of fractribution.
* ***`conversion_window_length`***: Number of days in the conversion window,
  leading up to the end date of the conversion window.

### Step 2: Defining marketing channels (e.g. `Paid_Search_Brand`, `Email`, etc):

Marketing channels are defined in `templates/channel_definitions.sql`. If the
default set are not suitable, use the instructions in the file to write your
own definitions.

Fractribution uses ad spend by channel to compute return on ad spend (ROAS).
Overwrite `templates/extract_channel_spend_data.sql` to enable ROAS reporting.
Note that the ad spend period should include both the conversion window, and
the preceding `path_lookback_days`.

### Step 3: Support for cross-device tracking:

GA360 tracks users at the device level with a `fullVisitorId`. If a user logs
into your site, you can send a custom `userId` to GA360, which is then
associated with the `fullVisitorId`. If two `fullVisitorId`s map to the same
`userId`, it can mean the user is logging in from two different devices.

There are two ways to send a `userId` to GA360. First, there is a top-level
`userId` field added specifically for this purpose. However, this userId may not
be present in the GA360 BigQuery table, even if it is set in GA360. So the
second way is to use a custom dimension (either hit-level or top-level).

Fractribution supports cross-device tracking by maintaining its own mapping
table of `fullVisitorId` to `userId`. Whenever a `userId` is present,
Fractribution will use that to group GA360 sessions. Otherwise, it falls back to
using the `fullVisitorId`.

The main flags for supporting cross-device tracking are:

* ***`update_fullvisitorid_userid_map`***: `True` to update the internal map
    from `fullVisitorId` to `userId`, and `False` otherwise. Default: `True`.
* ***`userid_ga_custom_dimension_index`***: If you use a custom dimension for
    storing the `userId` in Google Analytics, set the index here. Fractribution
    will automatically look for the top-level `userId` field, even if this index
    is defined.
* ***`userid_ga_hits_custom_dimension_index`***: If you use a hit-level custom
    dimension for storing the `userId` in Google Analytics, set the index here.
    Fractribution will automatically look for the top-level `userId` field, even
    if this index is defined.

If you maintain your own mapping of `fullVisitorId` to `userId`, overwrite the
script `templates/extract_fullvisitorid_userid_map.sql`.

## Fractribution Parameters:

* ***`project_id`***: Google Cloud `project_id` to run Fractribution inside.
* ***`dataset`***: BigQuery dataset to write the Fractribution output.
* ***`region`***: Region to create the dataset if it does not exist (see
  https://cloud.google.com/bigquery/docs/locations).
* ***`ga_sessions_table`***: Name of the GA360 BigQuery table in the format
  `<PROJECT>.<DATASET>.<TABLE>_*`.
* ***`hostnames`***: Comma separated list of hostnames. Restrict user sessions
  to this set of hostnames (Default: no restriction).
* ***`conversion_window_end_date`***: `'YYYY-MM-DD'` date in UTC time to define
  the end of the reporting period (inclusive) to look for a conversion.
* ***`conversion_window_end_today_offset_days`***: Set the conversion window end
  date to this many days before today. This is an alternative to
  `conversion_window_end_date` used in regular scheduled runs Fractribution.
* ***`conversion_window_length`***: Number of days in the conversion window.
* ***`path_lookback_days`***: Number of days in a user\'s path to
  (non)conversion. Recommended values: `30`, `14`, or `7`.
* ***`path_lookback_steps`***: Limit the number of steps / marketing channels in
  a user's path to (non)conversion to the most recent path_lookback_steps.
  (Default: no restriction).
* ***`update_fullvisitorid_userid_map`***: `True` to update the internal map
  from `fullVisitorId` to `userId`, and `False` otherwise. Default: `True`.
* ***`userid_ga_custom_dimension_index`***: If you use a custom dimension for
  storing the `userId` in Google Analytics, set the index here. Fractribution
  will automatically look for the top-level `userId` field, even if this index
  is defined.
* ***`userid_ga_hits_custom_dimension_index`***: If you use a hit-level custom
  dimension for storing the `userId` in Google Analytics, set the index here.
  Fractribution will automatically look for the top-level `userId` field, even
  if this index is defined.
* ***`path_transform`***: Fractribution extracts a path of marketing channels
  for each user. The path transform will change this path to improve
  matching and performance of the Fractribution algorithm on sparse data. For
  example, if a user has several Direct to website visits, this can be
  compressed to one representative Direct to website visit. There are 4
  transforms to choose from. Given a path of channels
  `(D, A, B, B, C, D, C, C)`, the transforms are:

    * ***`unique`***: (identity transform): yielding `(D, A, B, B, C, D, C, C)`,
    * ***`exposure`***: (collapse sequential repeats, default option):
    yielding `(D, A, B, C, D, C)`,
    * ***`first`***: (remove repeats): yielding `(D, A, B, C)`,
    * ***`frequency`***: (remove repeats, but keep a count): yielding
    `(D(2), A(1), B(2), C(3))`

  Path transforms can now be chained together and are executed in the order
  specified. To specify multiple transforms from the command line, use one
  separate --path_transform for each transform. Otherwise, pass in a list of
  strings, one per transform. Additional options for transform are:

    * ***`trimLongPath(n)`***:
    * ***`removeIfNotAll(channel)`***:
    * ***`removeIfLastAndNotAll(channel)`***:

  Both removeIfNotAll and removeIfLastAndNotAll are typically used to downweight
  the contribution of the 'Direct' / 'Direct-to-site' channel.
* ***`attribution_model`***: Which attribution model to use. Models include:
  `shapley`, `first_touch`, `last_touch`, `position_based` and `linear`.
  (Default: `shapley`).
* ***`templates_dir`***: Optional directory containing custom SQL templates.
  When loading a template, this directory is checked first before the default
  ./templates directory.
* ***`channel_definitions_sql`***: Optional argument to override the default
  filename of the SQL template for mapping channel definitions to channel names.
* ***`conversion_definition_sql`***: Optional argument to override the default
  filename of the SQL template that defines a conversion.
* ***`extract_conversions_sql`***: Optional argument to override the default
  filename of the SQL template for extracting all conversions.

## <a id="running-fractribution"></a>Tutorial: Running Fractribution on the Google Merchandise Store.

We will run Fractribution over the
[publicly-available GA360 dataset](https://support.google.com/analytics/answer/7586738?hl=en)
for the [Google Merchandise store](https://googlemerchandisestore.com/), a real
ecommerce store that sells Google-branded merchandise. You can view the
obfuscated data on BigQuery
[here](https://bigquery.cloud.google.com/table/bigquery-public-data:google_analytics_sample.ga_sessions_20170801).

The easiest way to run Fractribution is manually from the command line. This
works well for experimenting (e.g. with new conversion or channel definitions)
and debugging. If you want to setup Fractribution to run on a schedule though,
please see the following section on [Deploying Fractribution](#deploying-fractribution).


To run from the command line, begin by downloading the fractribution folder to
your local computer and then change directory into `fractribution/py`

Next, select values for the following:

* ***`<PROJECT_ID>`***: GCP project in which to run Fractribution
* ***`<DATASET>`***: BigQuery dataset name to store the Fractribution output.
* ***`<REGION>`***:
   [Region name](https://cloud.google.com/bigquery/docs/locations) in which to
   create ***`<DATASET>`*** if it doesn't already exist. E.g. us-central1



Then run the following command to authenticate with GCP:

```export GOOGLE_APPLICATION_CREDENTIALS=<CREDENTIALS_FILENAME>```

Finally, run Fractribution with the following command:

```
python3 main.py \
--project_id=<PROJECT_ID> \
--dataset=<DATASET>
--region=<REGION> \
--ga_sessions_table=bigquery-public-data.google_analytics_sample.ga_sessions_* \
--conversion_window_end_date=2017-08-01 \
--conversion_window_length=30 \
--path_lookback_days=30 \
--path_transform=exposure \
--attribution_model=shapley
```

Once the command finishes, go to your BigQuery ***`<DATASET>`*** and look at the
results, including the final report table.


### <a id="deploying-fractribution"></a>Deploying Fractribution on GCP: Cloud Functions vs VM

We recommend deploying Fractribution via Cloud Functions. Setup and maintenance
are easier, and because Cloud Functions are serverless, you only pay for what
you use. The main downsides are that Cloud Functions are limited to 2GB of RAM
and 9 minutes of runtime. If Cloud Functions run out of memory or time on your
data, switch to the VM approach, which allows you to select a compute engine
with much higher memory and no time limits.

Either way, as for the command line approach above, please select:

* ***`<PROJECT_ID>`***: GCP project in which to run Fractribution
* ***`<DATASET>`***: BigQuery dataset name to store the Fractribution output.
* ***`<REGION>`***:
   [Region name](https://cloud.google.com/bigquery/docs/locations) in which to
   create ***`<DATASET>`*** if it doesn't already exist. E.g. us-central1

### Approach 1: Running Python Cloud Functions (recommended)

#### Setup
[Install gcloud SDK](https://cloud.google.com/sdk/install)

```
gcloud auth login && gcloud config set project <PROJECT_ID>
```

Download the fractribution folder to your local computer and change directory
into `fractribution/py`. We will use the default definition of customer
conversion and revenue. We will also use the default channel definitions.
However, to make the report more interesting, in
`templates/extract_channel_spend_data.sql`, comment out the default SQL, and
uncomment the sample uniform spend data instead.

#### Deploying and Running the Fractribution Cloud Function

For this tutorial, we will create a Cloud Function called `FractributionTest`.

```
gcloud functions deploy FractributionTest \
--runtime python37 \
--region <REGION> \
--entry-point main \
--trigger-event google.pubsub.topic.publish \
--trigger-resource FractributionTestPubSub \
--timeout 540s \
--memory 2GB
```

The `trigger-*` flags above setup how to run `FractributionTest`. The
`trigger-resource` flags creates a PubSub topic called `FractributionTestPubSub`.
The Cloud Function executes when it receives a message on this topic.
To publish a message and trigger `FractributionTest`, use the following command:

```
gcloud pubsub topics publish FractributionTestPubSub --message '{
"project_id":"<PROJECT_ID>",
"dataset":"<DATASET>",
"region":"<REGION>",
"ga_sessions_table":"bigquery-public-data.google_analytics_sample.ga_sessions_*",
"conversion_window_end_date":"2017-08-01",
"conversion_window_length":30,
"path_lookback_days":"30",
"path_transform":"exposure",
"attribution_model":"shapley"
}'
```

You can now go to your BigQuery `<DATASET>` to view the output of the first
stage of fractribution. Note that the output tables all have the same suffix,
which is the `<conversion_window_end_date>`. This helps separate regular
scheduled runs of Fractribution over time. We recommend looking at:

* ***`report_table`***: Channel-level summary of attributed conversions,
   revenue, spend and ROAS.
* ***`path_summary_table`***: For each transfomed path, total number of
   conversions, non-conversions, revenue, and channel-level fractional
   attribution values out of 1.
* ***`channel_counts_table`***: Number of marketing events, aggregated by
  `channel`, `campaign`, `source` and `medium`

#### Scheduling Fractribution to run end-to-end on the latest data.

1.  Decide how often you want to run Fractribution.

    `CRON_SCHEDULE` = Schedule on which Fractribution will be executed in
    cron-unix format.

    Example: `15 1 1 * *` - Run every first day of the month at 1:15AM.

1.  Use ***`conversion_window_end_today_offset_days`*** instead of the fixed
    ***`conversion_window_end_date`*** in the parameters. Suggested values are
    `1` or `2`, to give enough time for the Google Analytics data tables to be
    fully ingested into BigQuery.

1.  Create a cron job to run Fractribution using Cloud Scheduler.

    ```
    gcloud scheduler jobs create pubsub Fractribution --schedule
    "<CRON_SCHEDULE>" --topic FractributionTest --message-body '{
    "project_id":"<PROJECT_ID>",
    "dataset":"<DATASET>",
    "region":"<REGION>",
    "ga_sessions_table":"<CLIENT_GA_TABLE_PATH>.ga_sessions_*",
    "conversion_window_end_today_offset_days":1,
    "conversion_window_length":30,
    <OTHER PARAMETERS HERE>
    }'
    ```

#### Debugging Fractribution

If you need to debug changes you've made, it is much faster to do locally,
rather than going through the slower process of uploading several versions of
the Cloud Function for each small change. The easiest way to debug is to use the
standalone command-line version of Fractribution, as
[described above](#running-fractribution). However, Cloud Functions do have a
local-execution framework called `functions-framework`, which is described
below:

First, follow
[these instructions](https://cloud.google.com/python/setup?hl=en#installing_python)
for installing python and running a virtual environment to sandbox dependencies.
In particular, from inside the `fractribution/py` directory:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=<CREDENTIALS_FILENAME>
functions-framework --target main --signature-type=event --debug
```

The Fractribution cloud function is now running in a local web server. Instead
of using PubSub, we `POST` the parameters to the Cloud Function using
***`curl`***. This means we have to encode the parameters in base64, e.g.

Linux:
```
echo -n '{"project_id":”<PROJECT_ID”, ...}' | base64 -w 0
```

Mac:
```
echo -n '{"project_id":”<PROJECT_ID”, ...}' | openssl base64
```

Windows PowerShell:
```
[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(‘{"project_id”:”<PROJECT_ID>”, ...}’))
```

Copy the encoded parameters text into the `curl` command below:

```
export GOOGLE_APPLICATION_CREDENTIALS=<CREDENTIALS_FILENAME>
curl -d '{"data": {"data": "<PUT_ENCODED_PARAMS_HERE>"}}' \-X POST -H "Content-Type: application/json" http://0.0.0.0:8080
```

That should capture both print statements and debug traces when things go wrong.

### Approach 2: Deploy Fractribution Docker Image on GCP VM Instance

1.  Variables:

    1.  `<Fractribution_Region>` - Region of the VM Instance
    1.  `<Fractribution_Zone>` - Zone of the VM Instance
    1.  `<Fractribution_Service_Account>` - Service Account for Fractribution.
        It should have the following roles:
        *  BigQuery Data Editor
        *  BigQuery Job User
        *  Compute Instance Admin (beta)
        *  Logs Writer
        *  Storage Object Viewer
    1.  `<Fractribution_Schedule>` - Schedule on which Fractribution will be
        executed in cron-unix format. Example: `"15 1 1 * *"` - Run every first
        day of the month at 1:15AM.
    1.  `<Fractribution_Param>` - Fractribution parameters for data and model
        in JSON format. Example value below:

        ```
        '{"project_id":"<PROJECT_ID>",
          "dataset":"<DATASET>",
          "region":"<REGION>",
          "ga_sessions_table":"bigquery-public-data.google_analytics_sample.ga_sessions_*",
          "conversion_window_end_date":"2017-08-01",
          "conversion_window_length":30,
          "path_lookback_days":30,
          "path_transform":"exposure",
          "attribution_model":"shapley"}'
        ```

1.  Create a docker image. From the Fractribution code directory:

    ```bash
    gcloud builds submit --tag gcr.io/<project_id>/<image-name>
    ```

1.  Set up the Compute Engine Instance

    1.  Go to the VM instances page
        https://console.cloud.google.com/compute/instances
    1.  Click Create instance.
    1.  Set the _Name_.
    1.  Click Add label. Enter `env` for _Key_ and `fractribution` for _Value_.
    1.  Select Region, then select `<Fractribution_Region>.en`
    1.  For _Zone_ select `<Fractribution_Zone>`.
    1.  Select Deploy a container image.
    1.  Specify the container image (`gcr.io/<project_id>/<image-name>`)
        created in Step #1.
    1.  Expand Advanced container options section.
    1. Under Environment variables, click Add variable. Enter
        `fractribution_param` for _NAME_ and `<Fractribution_Param>` for
        _VALUE_.
    1. Under Identity and API access, for Service account, select
        `<Fractribution_Service_Account>`.
    1. Click Create at the bottom of the page.

1.  Set up Cloud Function to start a VM instance. (Reference)

    1.  Go to the Cloud Functions page in the Cloud Console.
    1.  Click Create Function.
    1.  Set the Name to startInstancePubSub.
    1.  Leave Memory allocated at its default value.
    1.  For Trigger, select Cloud Pub/Sub.
    1.  For Topic, select Create new topic....
    1.  A New pub/sub topic dialog box should appear.
    1.  Under Name, enter start-instance-event.
    1.  Click Create to finish the dialog box.
    1. For Runtime, select Node.js 10.
    1. Above the code text block, select the index.js tab.
    1. Replace the starter code with the following code:

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
    1.  Replace the starter code with the following code:

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
      "dependencies": {
        "@google-cloud/compute": "^1.0.0"
      }
    }
    ```

    1.  For Function to execute, enter `startInstancePubSub`.
    1.  Click Create.

1.  Set up Cloud Scheduler to trigger Pub/Sub. (Reference)

    1.  Go to the Cloud Scheduler page in the Cloud Console.
    1.  Click Create Job.
    1.  Set the Name to `startup-fractribution-instance`.
    1.  For Frequency, enter `<Fractribution_Schedule>`.
    1.  For Timezone, select your desired country and timezone.
    1.  For Target, select Pub/Sub.
    1.  For Topic, enter `start-instance-event`.
    1.  For Payload, enter the following:
        `{"zone":"<Fractribution_Zone>","label":"env=fractribution"}`
    1.  Click Create.

Disclaimer: This is not an officially supported Google product.
