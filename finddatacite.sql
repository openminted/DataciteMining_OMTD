PRAGMA temp_store_directory = '.';


--create table docs as select 1 as id,c2 as text from (setschema 'c2' xmlparse root:sofa '{"sofa/@/sofastring":"Hello world."}' select * from stdinput());
create temp table docs
as select * from (setschema 'id, text' select jdictsplit(c1, 'id', 'text') from stdinput()) where text <>'' and text not null;

create temp table output_table as select docid, doi as id, sqroot(min(1.49,max(confidence))/1.5) as c1 from
(
select docid, doi, case when length(context) > 120 then round((length(titles) + conf*10)/(length(context)*1.0),2) else round(conf/10.0,2) end as confidence from (
select docid, doi, regexpcountuniquematches(bag,lower(regexpr('\W|_',context,' '))) + 2*regexprmatches(creator,lower(context)) + regexprmatches(publisher,lower(context)) as conf, regexprmatches(titles,lower(context)) as match, titles, context
from
    (
    select docid, lower(stripchars(middle,'_')) as mystart,
    prev||' '||middle||' '||next as context   from
    (setschema 'docid,prev,middle,next' select id as docid, textwindow2s(normalizetext(textreferences(text)),15,3,15) from docs)
    )
,titlesandtriples where mystart = words and match) where confidence > 0.28
union all
select docid, doi, 1 as confidence from
    ( setschema 'docid,middle' select id as docid, textwindow(comprspaces(filterstopwords(regexpr('(/|:)(\n)',text,'\1'))),0,0,'\b10.\d{4,5}/') from docs ),dois
    where normalizetext(stripchars(regexpr('(\b10.\d{4,5}/.*)',middle),'.,')) = normaldoi
) group by docid, doi;


select 
jdict("@graph", jgroup(
  	           jdict
    			("@id","datacite1Uri",
     			"@type","http://openminted.eu/ns/fi#Datacite", 
      			"http://openminted.eu/ns/fi#confidence", c1, 
      			"dataciteId",id)),
      "@context",jdict("datacite-info",jdict("@id","http://openminted.eu/ns/fi#datacite-info",
                                            "@type","oa:Annotation"),
                       "confidence",jdict("@id","http://openminted.eu/ns/fi#confidence",
                                          "@type","http://www.w3.org/2001/XMLSchema#double"),
                       "dataciteId",jdict("@id","http://openminted.eu/ns/fi#dataciteId"),
                       "hasTarget",jdict("@id","http://www.w3.org/ns/oa#hasTarget"),
		       "hasBody",jdict("@id","http://www.w3.org/ns/oa#hasBody",
                                        "@type" , "@id" ),
                       "oa" , "http://www.w3.org/ns/oa#",
    "owl" , "http://www.w3.org/2002/07/owl#",
    "rdf" , "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xsd" , "http://www.w3.org/2001/XMLSchema#",
    "rdfs" , "http://www.w3.org/2000/01/rdf-schema#"
                      )
     )  as c1
from output_table;
