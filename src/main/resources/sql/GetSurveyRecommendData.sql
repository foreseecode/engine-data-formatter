declare
  measurementId number := 3765007;
  fromDate date := '01-JAN-2012';
  toDate date := '01-JAN-2012';
  cursor cur is
    select r.respondentKey respondentKey,
           r.respondentid id,
           to_char(r.responsetimestamp, 'yyyy-mm-dd"T"hh24:mi:ss') || dbtimezone timestamp,
           q.questionid key,
           f.answervalue value
    from fact_respquestionvalue f, respondents r, questions q
    where f.daydate between fromDate and toDate
          and f.measurementkey = measurementId
          and f.respondentkey = r.respondentkey
          and f.questionkey = q.questionkey
          and upper(q.questionlabel) like '%RECOMMEND%'
    order by r.respondentkey, q.externalquestionid;
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
                         '", "value":' || rec.value ||'}');
  end loop;
  DBMS_OUTPUT.put_line (']}');
end;