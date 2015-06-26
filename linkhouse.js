var highland = require('highland')
var request = require('request')
var neo4j = require('node-neo4j')
var config = require('./config')

var db = new neo4j('http://' + config.neo4j.username + ':' + config.neo4j.password + '@localhost:7474')

var cypher = highland.wrapCallback(function (query, callback) {
    db.cypherQuery(query, function (error, results) {
	if (error) throw error
	callback(error, results.data)
    })
})

var http = highland.wrapCallback(function (location, callback) {
    request(location, function (error, response, body) {
        var errorStatus = (response.statusCode >= 400) ? new Error(response.statusCode) : null
        response.body = body
        callback(error || errorStatus, response)
    })
})

function lookupCompany(number) {
    return {
	uri: 'https://api.companieshouse.gov.uk/company/' + number,
	auth: { user: config.companiesHouseKey }
    }
}

function cypherCompany(response) {
    var company = JSON.parse(response.body)
    return 'MATCH (c {companyNumber: "' + company.company_number + '"}) SET '
	+ 'c.companyName = "' + company.company_name + '", '
        + 'c.companyType = "' + company.type + '", '
	+ 'c.companyStatus = "' + company.company_status + '", '
        + 'c.companyJurisdiction = "' + company.jurisdiction + '", '
        + 'c.companyCreationDate = "' + company.date_of_creation + '" '
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
    var companyNumber = companyOfficers.links.self.split('/')[2]
    return companyOfficers.items.reduce(function (a, officer, i) {
	var officerName = function () {
	    var nameSplit = officer.name.split(', ')
	    return nameSplit[1] + ' ' + nameSplit[0].charAt(0) + nameSplit[0].slice(1).toLowerCase()
	}()
	return a + ' '
	    + 'MERGE (o' + i + ':Individual {name: "' + officerName + '"}) SET '
	    + 'o' + i + '.nationality = "' + officer.nationality + '", '
	    + 'o' + i + '.countryOfResidence = "' + officer.country_of_residence + '", '
	    + 'o' + i + '.occupation = "' + officer.occupation + '", '
	    + 'o' + i + '.addressCareOf = "' + officer.address.care_of + '", '
	    + 'o' + i + '.addressPremises = "' + officer.address.premises + '", '
	    + 'o' + i + '.addressLine1 = "' + officer.address.address_line_1 + '", '
	    + 'o' + i + '.addressLine2 = "' + officer.address.address_line_2 + '", '
	    + 'o' + i + '.addressLocality = "' + officer.address.locality + '", '
	    + 'o' + i + '.addressRegion = "' + officer.address.region + '", '
	    + 'o' + i + '.addressPostCode = "' + officer.address.postal_code + '", '
	    + 'o' + i + '.addressCountry = "' + officer.address.country + '" '
	    + 'MERGE (o' + i + ')-[:IS_AN_OFFICER_OF {'
	    + 'role: "' + officer.officer_role + '", '
	    + 'appointmentDate: "' + officer.appointed_on + '", '
	    + 'resignationDate: "' + officer.resigned_on + '" '
	    + '}]->(c)'
    }, 'MATCH (c { companyNumber: "' + companyNumber + '" })')
}

highland(['MATCH (n) WHERE n.companyNumber <> "" RETURN n.companyNumber'])
    .flatMap(cypher)
    .map(lookupCompany)
    .flatMap(http)
    .map(cypherCompany)
    .flatMap(cypher)
    .map(lookupCompanyOfficers)
    .flatMap(http)
    .map(cypherCompanyOfficers)
    .flatMap(cypher)
    .each(console.log) // force a thunk, pull everything through the stream
