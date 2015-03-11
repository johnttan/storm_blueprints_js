var storm = require('node-storm');

var sentenceSpout = (function(){

  var sentences = ["my dog has fleas",
          "i like cold beverages",
          "the dog ate my homework",
          "don't have a cow man",
          "i don't think i like fleas"];


  var index = 0;
  return storm.spout(function(sync){
    var that = this;
    setTimeout(function(){
      self.emit([sentences[index]]);
      index = (index + 1) % sentences.length;
      sync();
    }, 1)
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
    counts[word] = counts[word] || 0;
    counts[word] ++;
  })
})()

setTimeout(function(){
  console.log(JSON.stringify(counts))
}, 5000)
