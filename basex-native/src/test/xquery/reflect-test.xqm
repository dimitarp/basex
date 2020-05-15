module namespace test = 'http://basex.org/reflection-tests';

(: HTML -> org.ccil.cowan.tagsoup:tagsoup :)
declare %unit:test function test:html-parser() {
    unit:assert-equals(html:parser(), 'TagSoup')
};
declare %unit:test function test:html-parse() {
    unit:assert-equals(html:parse('<html><body>test</html>')/html, <html><body>test</body></html>)
};

(: RelaxNG -> org.relaxng:jing :)
declare %unit:test function test:validate-rng() {
    let $doc := <book><page>This is page one.</page><page>This is page two.</page></book>
    let $schema := 'element book { element page { text }+ }'
    return unit:assert-equals(validate:rng($doc, $schema, true()), ())
};

(: XSD -> xerces :)
declare %unit:test function test:validate-xsd-processor() {
    unit:assert-equals(validate:xsd-processor(), 'Java')
};
declare %unit:test function test:validate-xsd-version() {
    unit:assert-equals(validate:xsd-version(), '1.0')
};
declare %unit:test function test:validate-xsd() {
    let $doc := <root/>
    let $schema := <xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'>
                     <xs:element name='root'/>
                   </xs:schema>
    return unit:assert-equals(validate:xsd($doc, $schema), ())
};

(: XSLT :)
declare %unit:test function test:xslt-processor() {
    unit:assert-equals(xslt:processor(), 'Java')
};
declare %unit:test function test:xslt-version() {
    unit:assert-equals(xslt:version(), '1.0')
};
declare %unit:test function test:xslt-transform-text() {
    let $doc := <a/>
    let $stylesheet := <xsl:stylesheet version='3.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
                         <xsl:output method='text'/>
                         <xsl:template match="/">123</xsl:template>
                       </xsl:stylesheet>
    return unit:assert-equals(xslt:transform-text($doc, $stylesheet), '123')
};

(: CatalogWrapper :)

(: JLine -> org.jline:jline-reader :)

(: LuceneStemmer :)
(: SnowballStemmer :)
(: WordnetStemmer :)
(: JapaneseTokenizer :)
(: UCACollation :)


(: JDBC :)
(: Java Modules and Calls :)
