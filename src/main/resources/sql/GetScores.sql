declare
    measurementId number := 3765007;
    fromDate date := '01-JAN-2012';
    toDate date := '01-JAN-2012';
    cursor cur is
        select r.respondentKey,
            r.respondentid id,
            to_char(cast(fr.daydate as timestamp), 'yyyy-mm-dd"T"hh24:mi:ss') || dbtimezone timestamp,
            l.modellatentid key,
            fr.latentscore value
        from fact_resplatentresults fr, respondents r, latents l
        where fr.daydate between fromDate and toDate
            and fr.measurementkey = measurementId
            and fr.respondentkey = r.respondentkey
            and fr.latentkey = l.latentkey;
begin
    DBMS_OUTPUT.put_line ('{"items":[');
    for rec in cur
    loop
        if cur%rowcount != 1 then
            DBMS_OUTPUT.put(',');
        end if;
        DBMS_OUTPUT.put_line('{"respondentKey":"' || rec.respondentKey ||
                '", "id":"' || rec.id ||
                '", "timestamp":"' || rec.timestamp ||
                '", "key":"' || rec.key ||
                '", "value":"' || rec.value ||'"}');
    end loop;
    DBMS_OUTPUT.put_line (']}');
end;