var A = require ('./utils/async')
var M = require ('./extractPaperFeatures')
var P = require ("process")

var S = new Set ()

async function process (root_dir, re) {
    var files = await A.lsdir (root_dir)
    for (f of files) {
        var path = `${root_dir}/${f}`
        s = await A.lstat (path)
        if (s.isFile ()) {
            if (f.match (re))
                if (! S.has (f)) {
	            abstr = await M.extract (path, ['PII', 'ISSN', 'Title', 'Abstract'])
                    if (abstr) {
                        
                        console.log (`${f}\t${abstr.PII}\t${abstr.ISSN}\t${abstr.Title.replace(/\t+/g, " ")}\t${abstr.Abstract.replace (/\t+/g, " ")}`)
			console.error (path)
                        S.add (f)
                    }
                }
        }
        else
            await process (path, re)
    }

}

exports.process = process

re = new RegExp (P.argv [2])
process ('/home/thierry/HL/data/out', re)

