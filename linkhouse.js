var highland = require('highland')
var request = require('request')
var neo4j = require('node-neo4j')
var esc = require('js-string-escape')
var config = require('./config')

var db = new neo4j('http://' + config.neo4j.username + ':' + config.neo4j.password + '@localhost:7474')

var cypher = highland.wrapCallback(function (query, callback) {
    db.cypherQuery(query, function (error, results) {
        if (error) throw error
        callback(error, results.data)
    })
})

var http = highland.wrapCallback(function (location, callback) {
    request(location, function (error, response) {
        var failure = error ? error : (response.statusCode >= 400) ? new Error(response.statusCode) : null
        callback(failure, response)
    })
})

function lookupCompany(number) {
    console.log('Looking up company ' + number + '...')
    return {
        uri: 'https://api.companieshouse.gov.uk/company/' + number,
        auth: { user: config.companiesHouseKey }
    }
}

function cypherCompany(response) {
    var company = JSON.parse(response.body)
    return 'MATCH (c {companyNumber: "' + company.company_number + '"}) SET '
        + 'c.companyName = "' + esc(company.company_name) + '", '
        + 'c.companyType = "' + esc(company.type) + '", '
        + 'c.companyStatus = "' + esc(company.company_status) + '", '
        + 'c.companyJurisdiction = "' + esc(company.jurisdiction) + '", '
        + 'c.companyCreationDate = "' + esc(company.date_of_creation) + '" '
        + 'RETURN c.companyNumber'
}

function lookupCompanyOfficers(number) {
    return {
        uri: 'https://api.companieshouse.gov.uk/company/' + number + '/officers?items_per_page=100', // todo what is the maximum items per page?
        auth: { user: config.companiesHouseKey }
    }
}

function cypherCompanyOfficers(response) {
    var companyOfficers = JSON.parse(response.body)
    if (companyOfficers.items.length === 0) throw new Error('Company has no officers!')
    var companyNumber = companyOfficers.links.self.split('/')[2]
    return companyOfficers.items.reduce(function (a, officer, i) {
        var officerName = function () {
            var nameSplit = officer.name.split(', ')
            return nameSplit[1] + ' ' + nameSplit[0].charAt(0) + nameSplit[0].slice(1).toLowerCase()
        }()
        return a + '\n'
            + 'MERGE (o' + i + ':Individual {name: "' + esc(officerName) + '"}) SET '
            + 'o' + i + '.nationality = "' + esc(officer.nationality) + '", '
            + 'o' + i + '.countryOfResidence = "' + esc(officer.country_of_residence) + '", '
                                        + 'o' + i + '.occupation = "' + esc(officer.occupation) + '", '
            + 'o' + i + '.addressCareOf = "' + esc(officer.address.care_of) + '", '
            + 'o' + i + '.addressPremises = "' + esc(officer.address.premises) + '", '
            + 'o' + i + '.addressLine1 = "' + esc(officer.address.address_line_1) + '", '
            + 'o' + i + '.addressLine2 = "' + esc(officer.address.address_line_2) + '", '
            + 'o' + i + '.addressLocality = "' + esc(officer.address.locality) + '", '
            + 'o' + i + '.addressRegion = "' + esc(officer.address.region) + '", '
            + 'o' + i + '.addressPostCode = "' + esc(officer.address.postal_code) + '", '
            + 'o' + i + '.addressCountry = "' + esc(officer.address.country) + '"'
            + '\n'
            + 'MERGE (o' + i + ')-[:IS_AN_OFFICER_OF {'
            + 'role: "' + esc(officer.officer_role) + '", '
            + 'appointmentDate: "' + esc(officer.appointed_on) + '", '
            + 'resignationDate: "' + esc(officer.resigned_on) + '" '
            + '}]->(c)'
    }, 'MATCH (c { companyNumber: "' + companyNumber + '" })')
}

highland(['MATCH (n) WHERE n.companyNumber <> "" RETURN n.companyNumber'])
    .flatMap(cypher)
    .flatten()
    .map(lookupCompany)
    .ratelimit(300, 300000)
    .flatMap(http)
    .map(cypherCompany)
    .flatMap(cypher)
    .map(lookupCompanyOfficers)
    .ratelimit(300, 300000)
    .flatMap(http)
    .map(cypherCompanyOfficers)
    .flatMap(cypher)
    .errors(function (e) { console.log('Error: ' + e.message)})
    .each(console.log) // force a thunk, pull everything through the stream
