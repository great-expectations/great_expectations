const fetch = require('node-fetch');
const removeMd = require('remove-markdown');
const packageS3URL = "https://superconductive-public.s3.us-east-2.amazonaws.com/static/gallery/package_manifests.json";
const algoliasearch = require("algoliasearch");
const client = algoliasearch(process.env.ALGOLIA_ACCOUNT, process.env.ALGOLIA_WRITE_KEY);
const packageAlgoliaIndex = process.env.PACKAGE_INDEX;
const packageExpecAlgoliaIndex = process.env.PACKAGE_EXPEC_INDEX;
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
        expecKeys.forEach(key => {
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
            };
            expectationDataset.push(expecData);
        });
    });
}

function uploadPackageToAlgolia(dataset) {
    packageIndex.saveObjects(dataset)
        .then(() => {
            console.log("data uploaded to package index", packageAlgoliaIndex);
            packageIndexSetting();
        })
        .catch(err => console.log(err))
}

function uploadPackageExpecToAlgolia(dataset) {
    packageExpecIndex.saveObjects(dataset)
        .then(() => {
            console.log("data uploaded to package expectations index", packageExpecAlgoliaIndex);
            expecIndexSetting();
        })
        .catch(err => console.log(err))
}

function packageIndexSetting() {
    packageIndex.setSettings({
        searchableAttributes: ["package_name", "description"],
        customRanking: [
            'asc(package_name)',
        ]
    })
        .then(() => { });
}

function expecIndexSetting() {
    console.log("Creating package expectations facets and search settings");
    packageExpecIndex.setSettings({
        attributesForFaceting: ["searchable(library_metadata.tags)",
            "searchable(package)", "searchable(engineSupported)"],
        maxFacetHits: 100,
        searchableAttributes: ["description.snake_name", "description.short_description"],
        customRanking: [
            'asc(description.snake_name)',
        ]
    })
        .then(() => { });
}
