create or replace function fnc_url_to_clob (
        p_url       in varchar2,
        p_header    in varchar2 default null,
        p_method    in varchar2 default 'GET',
        p_post_data in varchar2 default null)
return clob
is
    v_req         utl_http.req;
    v_rsp         utl_http.resp;
    v_out         varchar2(32767);
    v_hdr         varchar2(32767);
    v_resp_clob	  clob;
begin
    utl_http.set_wallet(null, NULL);
    v_req := utl_http.begin_request(url => p_url, method => p_method);
    --
    if p_header is not null then
        -- Assume it is a simple type:value
        if instr(p_header,':') > 0 then
            utl_http.set_header(v_req, substr(p_header,1,instr(p_header,':')-1), substr(p_header,instr(p_header,':')+1));
        end if;
    end if;
    --
    if p_post_data is not null then
        utl_http.set_header(v_req, 'content-length',lengthb(p_post_data));
        utl_http.write_text (v_req, p_post_data);
    end if;
    --
    v_rsp := utl_http.get_response(v_req);
    --
    dbms_lob.createtemporary(v_resp_clob, TRUE);
    begin
        loop
            utl_http.read_text(v_rsp, v_out, 32767);
            dbms_lob.writeappend(v_resp_clob, length(v_out), v_out);
        end loop;
    exception
        when utl_http.end_of_body then
            utl_http.end_response(v_rsp);
    end;
    utl_http.end_request(v_req);
    return v_resp_clob;
end fnc_url_to_clob;
/
