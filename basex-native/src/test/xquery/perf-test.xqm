module namespace test = 'http://basex.org/reflection-tests';

declare variable $test:dbname := 'test';
declare variable $test:dbfile := 'test.gz';
declare variable $test:dbfileurl := 'http://aiweb.cs.washington.edu/research/projects/xmltk/xmldata/data/dblp/dblp.xml.gz';

declare %updating %unit:before-module function test:download-file() {
  if (file:exists($test:dbfile)) then ()
  else file:write-binary($test:dbfile, fetch:binary($test:dbfileurl))
};

declare %updating %unit:after-module function test:cleanup() {
  db:drop($test:dbname)
};

declare %unit:test %updating function test:create-db() {
  db:create($test:dbname, $test:dbfile, (), map { 'ftindex': true() })
};

declare %unit:test function test:unique-authors() {
  sort(distinct-values(db:open($test:dbname)/dblp/*/author/text()))
};

declare %unit:test function test:title-ft-search() {
  db:open($test:dbname)/dblp/*[./title/text() contains text { 'dbms', 'distribute'} all using stemming]
};
