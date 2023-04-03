// load .env file (used while development) for loading env variables
require('dotenv').config()
const fetch = require('node-fetch')
const expecS3URL = process.env.ALGOLIA_S3_EXPECTATIONS_URL
const algoliasearch = require('algoliasearch')
const client = algoliasearch(process.env.ALGOLIA_ACCOUNT, process.env.ALGOLIA_WRITE_KEY)
const expecAlgoliaIndex = process.env.ALGOLIA_EXPECTATION_INDEX
const index = client.initIndex(expecAlgoliaIndex)

// Replica Index Names And Sorting Order Settings
const replicaIndexAndSettings = [
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_ALPHA_ASC_INDEX}`, ranking: ['asc(description.snake_name)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_ALPHA_DSC_INDEX}`, ranking: ['desc(description.snake_name)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_COVERAGE_ASC_INDEX}`, ranking: ['asc(coverage_score)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_COVERAGE_DSC_INDEX}`, ranking: ['desc(coverage_score)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_CREATED_ASC_INDEX}`, ranking: ['asc(created_at)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_CREATED_DSC_INDEX}`, ranking: ['desc(created_at)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_UPDATED_ASC_INDEX}`, ranking: ['asc(updated_at)']
  },
  {
    replica: `${process.env.ALGOLIA_EXPEC_REPLICA_UPDATED_DSC_INDEX}`, ranking: ['desc(updated_at)']
  }
]

// Main Index setSettings
const attributesForFaceting = ['searchable(library_metadata.tags)', 'searchable(engineSupported)', 'searchable(exp_type)', 'searchable(package)', 'searchable(metrics)', 'searchable(contributors)']
const maxFacetHits = 100
const searchableAttributes = ['description.snake_name', 'description.short_description']
const customRanking = ['asc(description.snake_name)']

// load data from S3
loadFromS3(expecS3URL).then(response => {
  console.log('Length of expectation loaded from S3', Object.keys(response).length)
  if (Object.keys(response).length > 0) {
    const algDataset = formatExpectation(response)
    console.log('Size of algolia dataset ', algDataset.length)
    if (algDataset.length == 0) {
      console.log('No records to push to algolia')
      return
    }
    console.log('Formatted expectation sample: ', algDataset[0])
    // return;
    deleteIndex(algDataset)
  }
}).catch((error) => {
  console.log('Error fetching data from s3', error)
})

// delete exisitng index
function deleteIndex (algData) {
  index.delete().then(() => {
    console.log('existing index is deleted')
    uploadToAlgolia(algData)
  }).catch((error) => {
    console.log('Error in deleting index', algDataset)
  })
}

// load data from S3
async function loadFromS3 (URL) {
  const response = await fetch(URL)
  return await response.json()
}

// Format expectations and prepare JSON which will be sent to algolia
function formatExpectation (ExpecData) {
  const ExpectationKeys = Object.keys(ExpecData)
  const dataset = []
  ExpectationKeys.forEach((key, i) => {
    const data = {}
    data.objectID = key
    data.library_metadata = ExpecData[key].library_metadata
    data.description = ExpecData[key].description
    data.execution_engines = ExpecData[key].execution_engines
    data.maturity_checklist = ExpecData[key].maturity_checklist
    data.backend_test_result_counts = ExpecData[key].backend_test_result_counts
    data.engineSupported = ExpecData[key].backend_test_result_counts.map((db) => db.backend)
    data.coverage_score = ExpecData[key].coverage_score
    data.created_at = ExpecData[key].created_at
    data.updated_at = ExpecData[key].updated_at
    data.exp_type = ExpecData[key].exp_type
    data.package = ExpecData[key].package
    data.metrics = []
    data.contributors = []
    // Flatten the metrics array to get all the metrics
    if (ExpecData[key].metrics) {
      ExpecData[key].metrics.forEach((metric) => {
        data['metrics'].push(metric.name)
      });
    }
    // Flatten the contributors array to get all the contributors
    if (ExpecData[key].library_metadata.contributors) {
      ExpecData[key].library_metadata.contributors.forEach((contributor) => {
        data['contributors'].push(contributor.replace(/@/g, ""))
      });
    }
    dataset.push(data)
  })
  return dataset
}

// Upload data to algolia index
function uploadToAlgolia (dataset) {
  index.saveObjects(dataset)
    .then(() => {
      console.log('Expectations data uploaded to algolia')
      mainIndexSetting(dataset)
    })
    .catch(err => console.log(err))
}

function mainIndexSetting (dataset) {
  index.setSettings({
    attributesForFaceting: attributesForFaceting,
    maxFacetHits: maxFacetHits,
    searchableAttributes: searchableAttributes,
    customRanking: customRanking,
    // Creating replica index
    replicas: replicaIndexAndSettings.map(replica => replica.replica)
  })
    .then(() => {
      console.log('facets created.')
      fetchAllAtrributes(dataset[0])
      // Creating replica index setsettings
      setReplicaSettings()
    }).catch((error) => {
      console.log('Error in index settings', error)
    })
}

function fetchAllAtrributes (data) {
  console.log('data is ', data)
  const existingId = [data.description.snake_name]
  const attributes = Object.keys(data)
  console.log('Attributes are', attributes)
  index.getObjects(existingId, {
    attributesToRetrieve: attributes
  })
    .then((results) => {
      console.log('fetching all attributes ', results)
      console.log('Successfully fetched sample record from algolia !')
    }).catch((error) => {
      console.log('getting error while fetching', error)
    })
};

// Replica Index Settings
function setReplicaSettings () {
  replicaIndexAndSettings.map((repli) => {
    const { replica, ranking } = repli
    client.initIndex(replica).setSettings({
      attributesForFaceting: attributesForFaceting,
      maxFacetHits: maxFacetHits,
      searchableAttributes: searchableAttributes,
      customRanking: ranking
    })
      .then(() => {
        console.log(`Replica: ${replica} configured`)
      })
  })
}
