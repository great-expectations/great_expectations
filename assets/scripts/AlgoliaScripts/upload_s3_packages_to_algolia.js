// load .env file (used while development) for env variables
require('dotenv').config();
const fetch = require('node-fetch');
const removeMd = require('remove-markdown');
const packageS3URL = process.env.ALGOLIA_S3_PACKAGES_URL;
const algoliasearch = require("algoliasearch");
const client = algoliasearch(process.env.ALGOLIA_ACCOUNT, process.env.ALGOLIA_WRITE_KEY);
const packageAlgoliaIndex = process.env.ALGOLIA_PACKAGE_INDEX;
const packageExpecAlgoliaIndex = process.env.ALGOLIA_PACKAGE_EXPEC_INDEX;
const packageIndex = client.initIndex(packageAlgoliaIndex);
const packageExpecIndex = client.initIndex(packageExpecAlgoliaIndex);

// Remove markdown function for package description field
function removeMarkdown(str) {
    const plainText = removeMd(str, {
        stripListLeaders: true, // strip list leaders (default: true)
        listUnicodeChar: '',     // char to insert instead of stripped list leaders (default: '')
        gfm: true,               // support GitHub-Flavored Markdown (default: true)
        useImgAltText: true      // replace images with alt-text, if present (default: true)
    });
    return plainText;
}

// Replica expectation Index Names And Sorting Order Settings
const expecReplicaIndexAndSettings = [
    {
        replica: `${process.env.ALGOLIA_PACK_EXPEC_REPLICA_ALPHA_ASC_INDEX}`, ranking: ['asc(description.snake_name)']
    },
    {
        replica: `${process.env.ALGOLIA_PACK_EXPEC_REPLICA_ALPHA_DSC_INDEX}`, ranking: ['desc(description.snake_name)']
    },
    {
        replica: `${process.env.ALGOLIA_PACK_EXPEC_REPLICA_COVERAGE_ASC_INDEX}`, ranking: ['asc(coverage_score)']
    },
    {
        replica: `${process.env.ALGOLIA_PACK_EXPEC_REPLICA_COVERAGE_DSC_INDEX}`, ranking: ['desc(coverage_score)']
    },
]

// Main Packages' Expectations Index setSettings
const expecAttributesForFaceting = ["searchable(library_metadata.tags)", "searchable(engineSupported)", "searchable(exp_type)", "searchable(package)"];
const maxFacetHits = 100;
const epxecSearchableAttributes = ["description.snake_name", "description.short_description"]
const mainExpecIndexRanking = ['asc(description.snake_name)']

let packageDataset = [];
let expectationDataset = [];

loadPackageFromS3(packageS3URL).then(response => {
    console.log("Packages loaded from S3", response.length);
    formatPackageData(response);
    console.log("Packages count: ", packageDataset.length);
    console.log("Expectations extracted from packages: ", expectationDataset.length);
    if (packageDataset.length == 0) {
        console.log("No packages found.. Exiting !")
        return
    }
    packageIndex.delete().then(() => {
        console.log("Refreshing data for Packages in index: ", packageAlgoliaIndex);
        uploadPackageToAlgolia(packageDataset);
    })
    if (expectationDataset.length == 0) {
        console.log("No expectations found.. Exiting !")
        return
    } else {
        packageExpecIndex.delete().then(() => {
            console.log("Refreshing data for package's expectations in index: ", packageExpecAlgoliaIndex);
            uploadPackageExpecToAlgolia(expectationDataset);
        })
    }
})

//load package data from S3
async function loadPackageFromS3(URL) {
    const response = await fetch(URL);
    return await response.json();
}

function formatPackageData(data) {
    data.forEach((package) => {
        let pdata = Object.assign({}, package);
        delete pdata.expectations;
        pdata["objectID"] = package.package_name;
        pdata["description"] = removeMarkdown(package.description);
        packageDataset.push(pdata);
        const expecKeys = Object.keys(package.expectations);
        expecKeys.forEach((key) => {
            const expectation = package.expectations[key];
            let expecData = {
                "objectID": key,
                "description": expectation.description,
                "engineSupported": expectation.backend_test_result_counts.map((db) => db.backend),
                "library_metadata": expectation.library_metadata,
                "maturity_checklist": expectation.maturity_checklist,
                "execution_engines": expectation.execution_engines,
                "package": package.package_name,
                "backend_test_result_counts": expectation.backend_test_result_counts,
                "coverage_score": expectation.coverage_score,
                "exp_type": expectation.exp_type,
            };
            expectationDataset.push(expecData);
        });
    });
}

//Upload package data to package index
function uploadPackageToAlgolia(dataset) {
    packageIndex.saveObjects(dataset)
        .then(() => {
            console.log("data uploaded to package index", packageAlgoliaIndex);
            packageIndexSetting();
        })
        .catch(err => console.log("Error uploading data to Package Index !!",err))
}

//Upload package expectation data to package expectation index
function uploadPackageExpecToAlgolia(dataset) {
    packageExpecIndex.saveObjects(dataset)
        .then(() => {
            console.log("data uploaded to package expectations index", packageExpecAlgoliaIndex);
            mainExpecIndexSetting();
        })
        .catch(err => console.log("Error uploading data to Package Expectatiions Index !!",err))
}

function packageIndexSetting() {
    packageIndex.setSettings({
        searchableAttributes: ["package_name", "description"],
        customRanking: [
            'asc(package_name)',
        ]
    })
        .then(() => {
            console.log("Package index settings done.")
        })
        .catch(err => console.log("Error creating settings for Package Index !!",err));
}

function mainExpecIndexSetting() {
    console.log("Creating package expectations replicas, facets and search settings");
    packageExpecIndex.setSettings({
        attributesForFaceting: expecAttributesForFaceting,
        maxFacetHits: maxFacetHits,
        searchableAttributes: epxecSearchableAttributes,
        customRanking: mainExpecIndexRanking,
        // Creating replica index
        replicas: expecReplicaIndexAndSettings.map(replica => replica.replica)
    })
        .then(() => {
            console.log('facets and replicas created. Now configuring expectation replica indices');
            // Creating replica index setsettings
            setExpecIndexReplicaSettings();
        })
        .catch(err => console.log("Error creating setting for Package Expectation Index !!", err));;
}

//Replica Index for expectations Settings
function setExpecIndexReplicaSettings() {
    expecReplicaIndexAndSettings.map((repli) => {
        const { replica, ranking } = repli;
        client.initIndex(replica).setSettings({
            attributesForFaceting: expecAttributesForFaceting,
            maxFacetHits: maxFacetHits,
            searchableAttributes: epxecSearchableAttributes,
            customRanking: ranking
        })
            .then(() => {
                console.log(`Replica: ${replica} configured !!`)
            })
            .catch(err => console.log(`Error creating settings for Packages' Expectation replica Index !! ${replica}`,err));
    })
}
