const fetch = require('node-fetch');
const expecS3URL = "https://superconductive-public.s3.us-east-2.amazonaws.com/static/gallery/expectation_library_v2.json";
const algoliasearch = require("algoliasearch");
const client = algoliasearch(process.env.ALGOLIA_ACCOUNT, process.env.ALGOLIA_WRITE_KEY);
const expecAlgoliaIndex = process.env.EXPECTATION_INDEX;
const index = client.initIndex(expecAlgoliaIndex);

loadFromS3(expecS3URL).then(response => {
    console.log("Length of expectation loaded from S3", Object.keys(response).length);
    if (Object.keys(response).length > 0) {
        let algDataset = formatExpectation(response);
        console.log('Size of algolia dataset ', algDataset.length)
        if (algDataset.length == 0) {
            console.log("No records to push to algolia");
            return;
        }
        console.log("Formatted expectation sample: ", algDataset[0]);
        deleteIndex(algDataset);
    }
}).catch((error) => {
    console.log('Error fetching data from s3', error);
})

//delete exisitng index
function deleteIndex(algData) {
    index.delete().then(() => {
        console.log('existing index is deleted');
        uploadToAlgolia(algData);
    }).catch((error) => {
        console.log("Error in deleting index", algDataset);
    });
}

//load data from S3
async function loadFromS3(URL) {
    const response = await fetch(URL);
    return await response.json();
}

function formatExpectation(ExpecData) {
    const ExpectationKeys = Object.keys(ExpecData);
    let dataset = [];
    ExpectationKeys.forEach((key, i) => {
        let data = {};
        data.objectID = key;
        data.library_metadata = ExpecData[key].library_metadata;
        data.description = ExpecData[key].description;
        data.execution_engines = ExpecData[key].execution_engines;
        data.maturity_checklist = ExpecData[key].maturity_checklist;
        data.backend_test_result_counts = ExpecData[key].backend_test_result_counts;
        data.engineSupported = ExpecData[key].backend_test_result_counts.map((db) => db.backend);
        dataset.push(data);
    })
    return dataset;
}

function uploadToAlgolia(dataset) {
    index.saveObjects(dataset)
        .then(() => {
            console.log('Expectations data uploaded to algolia');
            indexSetting(dataset);
        })
        .catch(err => console.log(err))
}

function indexSetting(dataset) {
    index.setSettings({
        attributesForFaceting: ["searchable(library_metadata.tags)", "searchable(engineSupported)"],
        maxFacetHits: 100,
        searchableAttributes: ["description.snake_name", "description.short_description"],
        customRanking: [
            'asc(description.snake_name)',
        ]
    })
        .then(() => {
            console.log('facets created.');
            fetchAllAtrributes(dataset[0]);
        }).catch((error) => {
            console.log("Error in index settings", error);
        });
}

function fetchAllAtrributes(data) {
    console.log("data is ", data);
    let existingId = [data.description.snake_name];
    let attributes = Object.keys(data);
    console.log("Attributes are", attributes);
    index.getObjects(existingId, {
        attributesToRetrieve: attributes
    })
        .then((results) => {
            console.log('fetching all attributes ', results);
            console.log("Successfully fetched sample record from algolia !");
        }).catch((error) => {
            console.log('getting error while fetching', error);
        })
};
