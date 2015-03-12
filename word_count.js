var storm = require('node-storm');
var fs = require('fs');

var sentenceSpout = (function(){

  var sentences = ["my dog has fleas",
          "i like cold beverages",
          "the dog ate my homework",
          "don't have a cow man",
          "i don't think i like fleas"];


  var index = 0;
  return storm.spout(function(sync){
    setTimeout(function(){
      this.emit([sentences[index]]);
      index = (index + 1) % sentences.length;
      sync();
    }.bind(this), 1)
  }).declareOutputFields(["word"])
})();

var splitSentenceBolt = storm.basicbolt(function(data){
  var words = data.tuple[0].split(' ');
  words.forEach(function(word){
    this.emit([word])
  }.bind(this))
}).declareOutputFields(["word"]);

var countBolt = (function(){
  var count = {};
  return storm.basicbolt(function(data){
    var word = data.tuple[0];
    count[word] = count[word] || 0;
    count[word]++;
    this.emit([word, count[word]]);
  }).declareOutputFields(["word", "count"])
})()

var counts = {};
var reportBolt = (function(){
  return storm.basicbolt(function(data){
    var word = data.tuple[0];
    counts[word] = counts[word] || data.tuple[1];
  })
})()

var builder = storm.topologybuilder();
builder.setSpout('sentences', sentenceSpout);
builder.setBolt('split_sentences', splitSentenceBolt, 8).shuffleGrouping('sentences');
builder.setBolt('word_count', countBolt, 12).fieldsGrouping('split_sentences', ['word']);
builder.setBolt('report', reportBolt).globalGrouping('word_count');

var nimbus = process.argv[2];
var options = {
  config: {'topology.debug': true}
}
var topology = builder.createTopology()
var cluster = storm.localcluster()
cluster.submit(topology, options).then(function() {
  return q.delay(500)
}).finally(function() {
  fs.writeFile('logs.txt', JSON.stringify(counts));
  return cluster.shutdown()
}).fail(console.error)

