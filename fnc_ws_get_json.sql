CREATE OR REPLACE FUNCTION fnc_ws_get_json (p_url in varchar2, p_auth in varchar2 default null) 
return clob is
    v_result clob;
begin
    apex_web_service.g_request_headers(1).name := 'Content-Type'; 
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Accept'; 
    apex_web_service.g_request_headers(2).value := 'application/json';
    if p_auth is not null then
        apex_web_service.g_request_headers(3).name := 'Authorization'; 
        apex_web_service.g_request_headers(3).value := p_auth;
    end if;
    v_result := APEX_WEB_SERVICE.MAKE_REST_REQUEST(
                    p_url               => p_url,
                    p_http_method       => 'GET',
                    p_wallet_path       => '');
    if APEX_WEB_SERVICE.g_status_code not in (200,201,202,204) then
        dbms_output.put_line('A:'||APEX_WEB_SERVICE.g_status_code);
        dbms_output.put_line('B:'||APEX_WEB_SERVICE.g_reason_phrase);
        dbms_output.put_line('C:'||p_url);
    end if;
    return v_result;
end fnc_ws_get_json;
/
