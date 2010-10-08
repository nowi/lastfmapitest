/**
 * User: nowi
 * Date: 19.09.2010
 * Time: 16:39:47
 */

@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.5.0')
import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.URLENC
// To download GMongo on the fly and put it at classpath

@Grab(group = 'com.gmongo', module = 'gmongo', version = '0.5.1')
import com.gmongo.GMongo
import net.sf.json.JSONNull

def qypeapikey = "0pw0yQ6ZF3fWhRg6WmNlAQ"
def qypeapisecret = "RZxgDkADuoi93Wr9ijeEuKYFia2jFFPfjHX6ph44Js8"

qype = new RESTClient('http://api.qype.com/v1/')

// list of fields we are intrested in, we exclue some because they caused serialization
// TODO error with mongo db, investigate this
//def properties = [
//        "categories",
//        "phone",
//        "updated",
//        "point",
//        "average_rating",
//        "address",
//        "title",
//        "url",
//
//]

def mongo = new GMongo("127.0.0.1", 27017)
// url --> backtweet json format
def db = mongo.getDB("qype")

// Removing all documents
println "Removing all documents"
db.places.remove([:])

// load all venues for berlin
def response = qype.get(path: "places.json", query: ['in': 'berlin', 'consumer_key': qypeapikey])

while(response != null) {
  println "Response from Qype : ${response.data.results}"
  response.data.results.place.each {jsonObject ->
    // TODO exclude for now the links property because it includes some garbage eventually
    def place = jsonObject.findAll {key, value -> key != "links"}
    db.places << place + ["_id": place.id]
    db.categories << place.categories + ["_id": place.id]
  }

  def nextUrl = response.data.links.find({it.rel == "next"})?.href
  // extract the page number
  def pageNo = extractPageNumber(nextUrl)

  if(nextUrl && pageNo) {
    response = qype.get(path: "places.json", query: ['in': 'berlin', 'consumer_key': qypeapikey,"page":pageNo])
  } else {
    response = null
  }

}

println "Finished fetching venues from Qype !"



def extractPageNumber(def url) {
  if(url != null && !url.isEmpty())
    return url.split("page=")[1]
  else
    return null
}