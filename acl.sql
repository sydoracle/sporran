BEGIN
  DBMS_NETWORK_ACL_ADMIN.append_host_ace (
    host       => '*.spotify.com', 
    lower_port => 80,
    upper_port => 443,
    ace        => xs$ace_type(privilege_list => xs$name_list('http'),
                              principal_name => 'spotify',
                              principal_type => xs_acl.ptype_db)); 
END;
/
