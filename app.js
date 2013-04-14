/**
 * Какой сам ⚝
 * Author: Pavlenov Semen
 * Date: 14.04.13 14:46
 *
 * E = mc^2
 */

var $cron = require('cron'),
    $mongoose = require('mongoose'),
    $tubur = require('node-tubur'),

    __endvars__;

$mongoose.connect('mongodb://localhost/stats');

global.dump = new $tubur.utils.Logger({ colors: true });

function rnd(min, max)
{
    return Math.floor(Math.random()*(max-min+1)+min);
}

var Schema = $mongoose.Schema,
    ObjectId = Schema.ObjectId;

var PerSchema = new Schema({

    creativeNmb: { type: 'number' },
    cost: { type: 'number' },
    click: { type: 'boolean', default: false },
    date: { type: 'Date', default: Date.now },
    check: { type: 'boolean', default: false }

});

var StatsSchema = new Schema({

    creativeNmb: { type: 'number' },
    cost: { type: 'number' },
    count: { type: 'number' }

});

PerSchema.statics.agg = function() {

    var self = this,
        timeLine = new Date( Date.now() - 5 * 1000),
        query = { check: false, date: { $lt: timeLine } };

    var o = {};
    o.query = query;
    o.map = function()
    {
        emit(this.creativeNmb, { cost: parseInt(this.cost), count: 1 });
    };

    o.reduce = function(key, values)
    {
        var sum = 0, count = 0;
        values.forEach(function(item) {

            sum += item.cost;
            count += 1;

        });
        return { sum: sum, count: count };
    };

    o.verbose = true;
//    o.out = { replace: 'der' };
    this.model('Per').mapReduce(o, function (err, model, stats) {

        if (err) dump.error(err);

//        self.model('Per').update(query, { check: true });

        dump.info(JSON.stringify(stats));

        dump.info(model);

//        model.find().exec(function(err, tempDocs) {
//
//            dump.info(tempDocs.length, tempDocs[0], tempDocs[2]);
//
//        });

    })

};

var PerModel = $mongoose.model('Per', PerSchema, 'per');
var StatModel = $mongoose.model('Stat', StatsSchema, 'stat');

(function loop() {

    setTimeout(function() {

        var currTime = new Date().getTime();

        PerModel.create({

            creativeNmb: rnd(100, 120),
            cost: rnd(5, 20),
            click: $tubur.utils.toBool(rnd(0, 1))

        }, function(err, perDoc) {

            if (err) return dump.error(err);

//            dump.info(perDoc);

        });

        return setTimeout(loop, 50);


    }, 0);

})();


var job = new $cron.CronJob({
    cronTime: '*/1 * * * *',
    onTick: function() {

        PerModel.agg();

    },
    start: false
});
job.start();