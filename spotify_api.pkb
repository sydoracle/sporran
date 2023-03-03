set define off

create or replace PACKAGE BODY spotify_api is
    --
    g_token varchar2(32000);
    --
    FUNCTION get_token return varchar2 is
        v_client_id     varchar2(1000);
        v_client_secret varchar2(1000);
        v_refresh_token varchar2(1000);
        v_access_token  varchar2(1000);
        v_response      varchar2(32000);
        v_expiry        number;
    BEGIN
        if g_expiry is not null
        and g_expiry < sysdate then 
            return g_token;
        end if;
        --
        select  max(case when config_name = 'CLIENT_ID' then config_value end) client_id,
                max(case when config_name = 'CLIENT_SECRET' then config_value end) client_secret,
                max(case when config_name = 'REFRESH_TOKEN' then config_value end) refresh_token
        into v_client_id, v_client_secret, v_refresh_token
        from spotify_config;
        --
        -- call the oauth_authenticate procedure to get a Token, 
        -- based on your Client_Id and Client_Secret and the existing refresh token
        v_response := fnc_url_to_clob (
                p_url       =>  'https://accounts.spotify.com/api/token',
                p_method    =>  'POST', 
                p_header    =>  'Content-Type: application/x-www-form-urlencoded', 
                p_post_data => 'client_id='||v_client_id||'&client_secret='||v_client_secret||'&grant_type=refresh_token&refresh_token='||v_refresh_token);
        --
        if v_response is JSON then
            select access_token, expires_in
            into  v_access_token, v_expiry
            from json_table(v_response, '$'
                        columns (
                            access_token        varchar2(400) PATH '$.access_token' null on empty error on error,
                            token_type          varchar2(400) PATH '$.token_type' null on empty error on error,
                            expires_in          number PATH '$.expires_in' null on empty error on error,
                            scope               varchar2(400) PATH '$.scope' null on empty error on error,
                            refresh_token       varchar2(400) PATH '$.refresh_token' null on empty error on error
                            )
                ) j;
        else
            raise_application_error(-20001,'Error:'||v_response);
        end if;
        --
        g_token := v_access_token;
        g_expiry := systimestamp + (v_expiry * interval '1' second);
        return v_access_token;
        --
    END get_token; 
    --
    procedure load_playlists is
        v_offset    number := 0;
        v_token     varchar2(2000);
    begin
        v_token := spotify_api.get_token;
        delete from playlists;
        while v_offset is not null loop
            insert into playlists
                (pl_id, pl_name, pl_owner_name, pl_desc, pl_tracks_url, pl_tracks_cnt, pl_json)
            select j.*
            from json_table(fnc_ws_get_json(
                        'https://api.spotify.com/v1/me/playlists?limit=50&offset='||v_offset,
                        'Bearer '||v_token),  '$.items[*]'
                        columns  (
                            pl_id           varchar2(800) PATH '$.id',
                            pl_name         varchar2(800) PATH '$.name',
                            pl_owner_name   varchar2(800) PATH '$.owner.display_name',
                            pl_desc         varchar2(2000) PATH '$.description',
                            pl_tracks_url   varchar2(800) PATH '$.tracks.href',
                            pl_tracks_cnt   number PATH '$.tracks.total',
                            pl_json  varchar2(4000) FORMAT JSON PATH '$')
                        ) j;
            dbms_session.sleep(2);
            if sql%rowcount = 0 then
                v_offset := null;
            elsif v_offset > 1000 then
                v_offset := null;
            else
                v_offset := v_offset + 50;
            end if;
        end loop;
    end load_playlists;
    --
    procedure user_tracks_to_playlist 
    is
        cursor c_main is
            select json_value(t_artists,'$[0].name') artist, 
                    listagg('spotify:track:'||t_id,',') within group (order by t_id) tracks
            from user_tracks
            where t_id not in (select pl.t_id from PLAYLIST_TRACKS pl)
            and t_added > sysdate - 7
            group by json_value(t_artists,'$[0].name')
            order by 1;
        v_res varchar2(32000);
        v_pl_id varchar2(100);
    begin
        --
        select pl_id 
        into v_pl_id
        from playlists
        where pl_name = 'Todo';
        --
        for r_main in c_main loop
            v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||v_pl_id||'/tracks?uris='||r_main.tracks,
                        p_method    =>  'POST', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => '{"position": 0}'
                        );
            dbms_session.sleep(2);
        end loop;
    end user_tracks_to_playlist;
    --
    procedure load_user_tracks is
        v_offset    number := 0;
        v_token     varchar2(2000);
    begin
        v_token := spotify_api.get_token;
        v_offset := 0;
        while v_offset <= 10000 loop
            merge into user_tracks d
            using (select added_seq, to_date(t_added,'YYYY-MM-DD"T"HH24:MI:SS"Z"') t_added, 
                       t_id, t_name, t_explicit, t_duration, 
                        t_album_name, t_album_id, t_release_dt, t_artists, t_album, t_track_json
                  from json_table(fnc_ws_get_json('https://api.spotify.com/v1/me/tracks?limit=50&offset='||v_offset, 
                        'Bearer '||v_token), '$.items[*]'
                      columns  (
                          added_seq   FOR ORDINALITY,
                          t_added		   varchar2(60)  PATH '$.added_at',
                          t_id           varchar2(800) PATH '$.track.id',
                          t_name         varchar2(800) PATH '$.track.name',
                          t_explicit     varchar2(800) PATH '$.track.explicit',
                          t_duration     number        PATH '$.track.duration_ms',
                          t_album_name   varchar2(800)  PATH '$.track.album.name',
                          t_album_id     varchar2(800)  PATH '$.track.album.id',
                          t_release_dt   varchar2(800)  PATH '$.track.album.release_date',
                          t_artists      varchar2(4000) FORMAT JSON PATH '$.track.artists',
                          t_album        varchar2(4000) FORMAT JSON PATH '$.track.album',
                          t_track_json   clob FORMAT JSON PATH '$' error on error
                          )
                      ) j
                  where t_name is not null) s
            ON (s.t_id = d.t_id )
            WHEN MATCHED THEN UPDATE 
               SET d.as_of_date = sysdate
            WHEN NOT MATCHED THEN INSERT 
                  (added_seq, t_added, t_id, t_name, t_explicit, t_duration, 
                  t_album_name, t_album_id, t_release_dt, t_artists, t_album, t_track_json)
               VALUES (s.added_seq, s.t_added, s.t_id, s.t_name, s.t_explicit, s.t_duration, 
                  s.t_album_name, s.t_album_id, s.t_release_dt, s.t_artists, s.t_album, s.t_track_json);
            --
            v_offset := v_offset + 50;
            commit;
            dbms_session.sleep(2);
            --
        end loop;
        --
        user_tracks_to_playlist;
        commit;
        --
    end load_user_tracks;
    --
    procedure load_pl_tracks is
        v_offset    number := 0;
        v_token     varchar2(2000);
        cursor c_pl is
            select pl_id, pl_tracks_url, pl_tracks_cnt, pl_name, pl_owner_name
            from playlists
            order by pl_owner_name, pl_name, pl_id;
    begin
        v_token := spotify_api.get_token;
        for r_pl in c_pl loop
            dbms_application_info.set_client_info(r_pl.pl_owner_name||'.'||r_pl.pl_name);
            dbms_output.put_line(r_pl.pl_owner_name||'.'||r_pl.pl_name||' '||to_char(sysdate,'HH24:MI:SS'));
            delete from playlist_tracks
            where pl_id = r_pl.pl_id;
            --
            v_offset := 0;
            while v_offset <= r_pl.pl_tracks_cnt loop
                insert into playlist_tracks
                    (pl_id, playlist_seq, t_id, t_name, 
                    t_explicit, t_dur_mins, t_album_name, t_album_id, t_release_dt, 
                    t_artists, t_album, t_track_json, added_at)
                select  r_pl.pl_id pl_id, j.playlist_seq, j.t_id, j.t_name, 
                        j.t_explicit, round(j.t_duration/(1000*60),1) t_dur_mins, 
                        j.t_album_name, j.t_album_id, j.t_release_dt, 
                        j.t_artists, j.t_album, j.t_track_json,
                        to_date(t_added,'YYYY-MM-DD"T"HH24:MI:SS"Z"') t_added
                from json_table(fnc_ws_get_json(r_pl.pl_tracks_url||'?limit=50&offset='||v_offset, 'Bearer '||v_token),
                        '$.items[*]'
                        columns  (
                            playlist_seq   FOR ORDINALITY,
                            t_added        varchar2(80)  PATH '$.added_at',
                            t_id           varchar2(800) PATH '$.track.id',
                            t_name         varchar2(800) PATH '$.track.name',
                            t_explicit     varchar2(800) PATH '$.track.explicit',
                            t_duration     number        PATH '$.track.duration_ms',
                            t_album_name   varchar2(800)  PATH '$.track.album.name',
                            t_album_id     varchar2(800)  PATH '$.track.album.id',
                            t_release_dt   varchar2(800)  PATH '$.track.album.release_date',
                            t_artists      varchar2(4000) FORMAT JSON PATH '$.track.artists',
                            t_album        varchar2(4000) FORMAT JSON PATH '$.track.album',
                            t_track_json   clob FORMAT JSON PATH '$.track' error on error
                            )
                        ) j;
                v_offset := v_offset + 50;
                dbms_session.sleep(2);
            end loop;
            --
        end loop;
    end load_pl_tracks;
    --
    procedure load_artists is
        v_ids   varchar2(4000);
        v_cnt   number := 9999;
    begin
        while v_cnt > 0 loop
            select listagg(a_id,',') within group (order by a_id) ids, count(*)
            into v_ids, v_cnt
            from
                (select distinct a_id
                from playlist_tracks t
                    join playlists p on p.pl_id = t.pl_id
                    outer apply json_table(t.t_artists,'$[*]'
                            columns (
                                a_name        varchar2(400) PATH '$.name' null on empty error on error,
                                a_type        varchar2(400) PATH '$.type' null on empty error on error,
                                a_id          varchar2(400) PATH '$.id' null on empty error on error)
                            ) j
                where a_id not in (select art_id from artist)
                fetch first 50 rows only);
            --
            if v_cnt > 0 then
                insert into artist
                    (art_id, art_name, art_popularity, art_follower_cnt, art_genre, art_json)
                select j.art_id, j.art_name, j.art_popularity, j.art_follower_cnt, j.art_genre, j.art_json
                from json_table(
                    fnc_ws_get_json('https://api.spotify.com/v1/artists?ids='||v_ids, 'Bearer '||spotify_api.get_token),
                        '$.artists[*]'
                            columns  (
                                art_id           varchar2(800) PATH '$.id',
                                art_name         varchar2(800) PATH '$.name',
                                art_popularity   NUMBER PATH '$.popularity',
                                art_follower_cnt NUMBER PATH '$.followers.total',
                                art_genre        varchar2(800) FORMAT JSON PATH '$.genres',
                                art_json         varchar2(4000) FORMAT JSON PATH '$')
                    ) j;
                dbms_session.sleep(2);
            end if;
        end loop;
    end load_artists;
    --
    procedure load_track_detail is
        v_ids   varchar2(4000);
        v_cnt   number := 9999;
		cursor c_dtl is
			select j.*
			from json_table(
					fnc_ws_get_json('https://api.spotify.com/v1/audio-features?ids='||v_ids, 'Bearer '||spotify_api.get_token),
					'$.audio_features[*]'
						columns  (
							t_id                  varchar2(200) PATH '$.id',
							acousticness          NUMBER PATH '$.acousticness'    ,
							danceability          NUMBER PATH '$.danceability'    ,
							energy                NUMBER PATH '$.energy'          ,
							instrumentalness      NUMBER PATH '$.instrumentalness',
							pitch_key             NUMBER PATH '$.key'             ,
							liveness              NUMBER PATH '$.liveness'        ,
							loudness              NUMBER PATH '$.loudness'        ,
							major_minor           NUMBER PATH '$.mode'            ,
							speechiness           NUMBER PATH '$.speechiness'     ,
							tempo                 NUMBER PATH '$.tempo'           ,
							time_signature        NUMBER PATH '$.time_signature'  ,
							valence               NUMBER PATH '$.valence'         ,
							features_json         varchar2(4000) FORMAT JSON PATH '$')
				) j;
    begin
		--
		insert into tracks
			( t_name       , t_album_name , t_dur_mins , t_explicit ,
			  artist_names , t_id         , t_album_id , artist_ids )
		select distinct t_name, t_album_name, t_dur_mins, t_explicit,
				json_query(t.t_artists, '$[*].name' WITH WRAPPER) artist_names,
				t_id, t_album_id,
				json_query(t.t_artists, '$[*].id' WITH WRAPPER) artist_ids
		from playlist_tracks t
		where t_id not in (select t_id from tracks);
		--
		commit;
		--
        while v_cnt > 0 loop
            select listagg(t_id,',') within group (order by t_id) ids, count(*)
			into v_ids, v_cnt
            from
                (select t_id
                from tracks t
                where t_features is null
                fetch first 50 rows only);
            --
            if v_cnt > 0 then
				v_cnt := 0;
				for r_dtl in c_dtl loop
					update tracks t
					set t_features       = r_dtl.features_json    ,
						 acousticness     = r_dtl.acousticness     ,
						 danceability     = r_dtl.danceability     ,
						 energy           = r_dtl.energy           ,
						 instrumentalness = r_dtl.instrumentalness ,
						 pitch_key        = r_dtl.pitch_key        ,
						 liveness         = r_dtl.liveness         ,
						 loudness         = r_dtl.loudness         ,
						 major_minor      = r_dtl.major_minor      ,
						 speechiness      = r_dtl.speechiness      ,
						 tempo            = r_dtl.tempo            ,
						 time_signature   = r_dtl.time_signature   ,
						 valence          = r_dtl.valence
					where t.t_id = r_dtl.t_id
					and t_features is null
					and r_dtl.features_json is not null;
					v_cnt := v_cnt + sql%rowcount;
				end loop;
			end if;
			--
			commit;
			--
            dbms_session.sleep(2);
        end loop;
    end load_track_detail;
    --
    procedure set_pl_desc_by_folder 
    is
      cursor c_main is
      select fp.folder_name, fp.pl_id, fp.pl_name, p.pl_desc
      from folder_playlists fp
                 left outer join playlists p on fp.pl_id = p.pl_id
      where p.pl_desc is null
      order by fp.folder_name;
   begin
      for r_pl in c_main loop
         dbms_output.put_line(':'||
            fnc_url_to_clob (
               p_url       =>  'https://api.spotify.com/v1/playlists/'||r_pl.pl_id,
               p_method    =>  'PUT', 
               p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
               p_post_data => '{"description": "'||r_pl.folder_name||'"}'
               )
         );
      end loop;
   end set_pl_desc_by_folder;
   --
   procedure tracks_to_playlist (
      p_dest_pl   in varchar2,
      p_src_pl    in varchar2 default null,
      p_t_id      in varchar2 default null, 
      p_keep_yn   in varchar2 default 'N',
      p_artist    in varchar2 default null)
    is
      cursor c_main is
         select   listagg(distinct 'spotify:track:'||t.t_id,',') within group (order by t.t_id) tracks,
                  json_object(key 'tracks' value json_arrayagg(json_object(key 'uri' value 'spotify:track:'||t.t_id) )) json_tracks
         from tracks t 
               left outer join playlist_tracks pt on t.t_id = pt.t_id
               left outer join playlists p on pt.pl_id = p.pl_id
         where (instr(p_t_id,t.t_id) > 0 or p_t_id is null)
         and   (p.pl_id = p_src_pl or p_src_pl is null)
         and   (p_artist is null 
                    or instr(artist_names, '"'||translate(p_artist,' "',' ')||'"') > 0 )
         -- and t.tempo < 90 p.pl_name like 'Todo'
         order by tempo;
        v_res varchar2(32000);
        v_pl_id varchar2(100);
    begin
        --
        for r_main in c_main loop
            v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||p_dest_pl||'/tracks?uris='||r_main.tracks,
                        p_method    =>  'POST', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => '{"position": 0}'
                        );
            dbms_session.sleep(2);
            if p_keep_yn = 'N' then
               v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||p_src_pl||'/tracks',
                        p_method    =>  'DELETE', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => r_main.json_tracks
                        );
            end if;
        end loop;
    end tracks_to_playlist;
    --
    procedure artist_albums is
        cursor c_main is
            select distinct art_name, art_id, album_cnt
            from artist a
            where album_cnt <= 5000
            and art_id not in (select b.art_id from artist_albums b where b.art_id is not null)
            order by album_cnt, art_name;
        cursor c_art is
            select art_name, art_id, album_cnt, cnt_as_of_date
            from artist a
            where (album_cnt is null or cnt_as_of_date < sysdate - 7)
            for update of album_cnt;
        v_cnt number;
        v_ix number;
    begin
        --
        for r_art in c_art loop
            select api_total
            into v_cnt
            from json_table(fnc_ws_get_json(
                    'https://api.spotify.com/v1/artists/'||r_art.art_id||'/albums?market=AU&limit=1&offset=0',
                    'Bearer '||spotify_api.get_token),  
                '$'
                    columns  (
                        api_next  varchar2(4000) PATH '$.next',
                        api_prev  varchar2(4000) PATH '$.previous',
                        api_total  varchar2(4000) PATH '$.total',
                        api_offset  varchar2(4000) PATH '$.offset'
                        )
                ) j;
            --
            update artist
            set  album_cnt = v_cnt, cnt_as_of_date = sysdate
            where current of c_art;
            dbms_session.sleep(1);
        end loop;
        --
        commit;
        --
        for r_main in c_main loop
            v_ix := 0;
            while v_ix < r_main.album_cnt loop
                insert into artist_albums 
                    (art_name, art_id, album_json)
                select r_main.art_name, r_main.art_id, artist_dtl_json
                from json_table(fnc_ws_get_json(
                        'https://api.spotify.com/v1/artists/'||r_main.art_id||'/albums?market=AU&limit=50&offset='||v_ix,
                        'Bearer '||spotify_api.get_token),  
                    '$'
                    columns  (
                        api_next  varchar2(4000) PATH '$.next',
                        api_prev  varchar2(4000) PATH '$.previous',
                        api_total  varchar2(4000) PATH '$.total',
                        NESTED PATH '$.items[*]' COLUMNS (artist_dtl_json  varchar2(4000) FORMAT JSON PATH '$'),
                        api_offset  varchar2(4000) PATH '$.offset'
                        )
                    ) j;
                v_ix := v_ix + 50;
            end loop;
            if c_main%rowcount/10 = trunc(c_main%rowcount/10) then
                dbms_session.sleep(1);
            end if;
        end loop;
        commit;
        --
        update  artist_albums
        set album_type = json_value(album_json,'$.album_type'),
                album_group = json_value(album_json,'$.album_group') ,
                id = json_value(album_json,'$.id'),
                name = json_value(album_json,'$.name')
        where id is null;
        --
        commit;
    exception
        when others then
            commit;
            raise_application_error(-20001,sqlerrm,true);
    end  artist_albums;
    --
	procedure track_search (p_term in varchar2) is
		v_search_cnt	number;
		v_ins_cnt		number;
		v_loop_cnt		number(2) := 1;
		v_ret 			clob;
		v_url 			varchar2(500) := 'https://api.spotify.com/v1/search?q=track:#SEARCH#&type=track&market=AU&limit=50&offset=';
	begin
		v_ret := fnc_ws_get_json(utl_url.escape(replace(v_url||'0','#SEARCH#',p_term)),'Bearer '||spotify_api.get_token);
        --
        select json_value(v_ret,'$.tracks.total')
        into v_search_cnt
        from dual;
        --
        if v_search_cnt > 999 then
            v_search_cnt := 999;
        end if;
        --
		v_ins_cnt := 0;
		while v_ins_cnt < v_search_cnt loop
			v_loop_cnt := v_loop_cnt + 1;
            --
			insert into spotify_search_result
				(album_id, album_name, album_reldt, explicit, track_id, track_name, track_popular, artist_json, item_json)
			select album_id, album_name, album_reldt, explicit, track_id, track_name, track_popular, artist_json, item_json
			from json_table(v_ret,  '$.tracks.items[*]'
				columns  (
					album_id        varchar2(100) PATH '$.album.id',
					album_name      varchar2(800) PATH '$.album.name',
					album_reldt     varchar2(100) PATH '$.album.release_date',
					explicit        varchar2(50)  PATH '$.explicit',
					track_id        varchar2(100) PATH '$.id',
					track_name      varchar2(800) PATH '$.name',
					track_popular   varchar2(100) PATH '$.popularity',
					artist_json     varchar2(4000) FORMAT JSON PATH '$.artists',
					item_json       varchar2(4000) FORMAT JSON PATH '$'
					)
				) j;
            if sql%rowcount = 0 then
                --Assume we are done
                v_ins_cnt := v_search_cnt;
            else
                --Increment 
                v_ins_cnt := v_ins_cnt + sql%rowcount;
                -- If we expect, request again
                if v_ins_cnt < v_search_cnt
                and sql%rowcount > 1 then
                    dbms_session.sleep(1);
                    if v_ins_cnt >= 950 then
                        v_ins_cnt := 949;
                    end if;
                    v_ret := fnc_ws_get_json(utl_url.escape(replace(v_url||(v_ins_cnt+1),'#SEARCH#',p_term)),
                        'Bearer '||spotify_api.get_token);
                else
                    v_ret := null;
                end if;
            end if;
		end loop;
		dbms_output.put_line(p_term||':'||v_ins_cnt);
	end track_search;
    --
    procedure del_dups is
      cursor c_main is
		with
			pt as
				(select p.pl_id, p.pl_name, p.pl_desc, pt.t_name, pt.t_explicit, pt.t_id, pt.added_at
				from playlists p
						join playlist_tracks pt on pt.pl_id = p.pl_id 
				where p.pl_owner_name = 'Gary'
				and p.pl_name not in ('Comedy',' Audiobooks','2020 Top','Doppelganger','Man Cave')
				and regexp_substr(nvl(p.pl_desc,'?'),'[^ ]+') not in ('Dedications','Premade','Themed')
				),
			dup as
				(select pt.t_name, t_id, 
						min(pl_id) keep (DENSE_RANK FIRST ORDER BY added_at asc)    first_pl_id,
						min(pl_name) keep (DENSE_RANK FIRST ORDER BY added_at asc)  first_pl_name,
						min(pl_desc) keep (DENSE_RANK FIRST ORDER BY added_at asc)  first_pl_desc,
						min(added_at) keep (DENSE_RANK FIRST ORDER BY added_at asc) first_pl_add,
						min(pl_id) keep (DENSE_RANK FIRST ORDER BY added_at desc)    last_pl_id,
						min(pl_name) keep (DENSE_RANK FIRST ORDER BY added_at desc)  last_pl_name,
						min(pl_desc) keep (DENSE_RANK FIRST ORDER BY added_at desc)  last_pl_desc,
						min(added_at) keep (DENSE_RANK FIRST ORDER BY added_at desc) last_pl_add
				from pt
				group by t_id, pt.t_name
				having count(distinct pl_id) = 2)
		select first_pl_name, first_pl_id, 
				listagg(distinct 'spotify:track:'||t_id,',') within group (order by t_id) tracks,
				json_object(key 'tracks' value json_arrayagg(json_object(key 'uri' value 'spotify:track:'||t_id) )) json_tracks
		from dup
		group by first_pl_name, first_pl_id;
        v_res varchar2(32000);
        v_pl_id varchar2(100);
    begin
        --
        for r_main in c_main loop
               v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||r_main.first_pl_id||'/tracks',
                        p_method    =>  'DELETE', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => r_main.json_tracks
                        );
        end loop;
    end del_dups;
    --
   procedure order_tracks (p_name in varchar2 default '%') is
      cursor c_main is
		with
			pt as
				(select p.pl_id, p.pl_name, p.pl_desc, pt.t_name, pt.t_explicit, pt.t_id, pt.added_at,
                        json_value(t_artists,'$[0].name') primary_artist
				from playlists p
						left outer join playlist_tracks pt on pt.pl_id = p.pl_id 
				where p.pl_owner_name = 'Gary'
				and p.pl_name not in ('Comedy',' Audiobooks','2020 Top','Doppelganger','Man Cave')
                and p.pl_name like nvl(p_name,'%')
				and regexp_substr(nvl(p.pl_desc,'?'),'[^ ]+') not in ('Dedications','Premade','Themed')
				)
        select pl_desc, pl_name, pl_id, count(*) c_all,
                listagg(distinct 'spotify:track:'||t_id,',') within group (order by primary_artist, t_name, t_id, added_at) tracks
                /*,
                json_object(key 'uris' value json_arrayagg(
                        json_object(key 'uri' value 'spotify:track:'||t_id) order by primary_artist, t_name, t_id, added_at
                    )) json_tracks
                */
        from pt
        group by pl_desc, pl_name, pl_id
        order by 1,2,3;
        v_res varchar2(32000);
        v_pl_id varchar2(100);
    begin
        --
        for r_main in c_main loop
               v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||r_main.pl_id||'/tracks?uris='||r_main.tracks,
                        p_method    =>  'PUT', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => null--r_main.json_tracks
                        );
                dbms_output.put_line(r_main.pl_desc||':'||r_main.pl_name||':'||v_res);
                dbms_session.sleep(1);
        end loop;
    end order_tracks;
    --
    procedure balance_playlists (p_pl_name1 in varchar2, p_pl_name2 in varchar2)
    is
        v_res varchar2(32000);
        cursor c_main is
            with
                pt as
                    (select p.pl_id, p.pl_name, p.pl_desc, pt.t_name, pt.t_explicit, pt.t_id, pt.added_at,
                            json_value(t_artists,'$[0].name') artist_0,
                            json_value(t_artists,'$[1].name') artist_1
                    from playlists p
                            left outer join playlist_tracks pt on pt.pl_id = p.pl_id 
                    where p.pl_name in (p_pl_name1,p_pl_name2)
                    and p.pl_owner_name = 'Gary'
                    ),
                g as
                    (select mod(ora_hash(artist_0,10),2) grp, count(*) cnt,
                            listagg(distinct 'spotify:track:'||t_id,',') within group (order by artist_0, t_name, t_id, added_at) tracks
                    from pt
                    where t_id is not null
                    group by mod(ora_hash(artist_0,10),2)
                    ),
                p as
                    (select min(pl_id) pl_id1, max(pl_id) pl_id2
                    from pt
                    having count(distinct pl_id) = 2)
            select g.grp, decode(g.grp,1,pl_id1,pl_id2) dest_pl, g.cnt, g.tracks
            from g cross join p;
    begin
        for r_main in c_main loop
           v_res := fnc_url_to_clob (
                    p_url       =>  'https://api.spotify.com/v1/playlists/'||r_main.dest_pl||'/tracks?uris='||r_main.tracks,
                    p_method    =>  'PUT', 
                    p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                    p_post_data => null--r_main.json_tracks
                    );
            dbms_output.put_line(r_main.dest_pl||':'||r_main.cnt||':'||v_res);
            dbms_session.sleep(1);
        end loop;
    end balance_playlists;
    --
    procedure get_follows is
        v_cnt       number := 999;
        v_start     varchar2(100);
    begin
        while v_cnt > 1 loop
            --
            select max('&after='||a_id) id
            into v_start
            from followed_artists
            where as_of_time >= sysdate -(1/24);
            --
            MERGE INTO followed_artists D
               USING (select j.*
                    from json_table(
                            fnc_ws_get_json('https://api.spotify.com/v1/me/following?type=artist'||v_start, 'Bearer '||spotify_api.get_token),
                                '$.artists.items[*]'
                                    columns  (
                                        art_id           varchar2(800) PATH '$.id',
                                        art_name         varchar2(800) PATH '$.name',
                                        art_popularity   NUMBER PATH '$.popularity',
                                        art_follower_cnt NUMBER PATH '$.followers.total',
                                        art_type         VARCHAR2 PATH '$.type',
                                        art_genre        varchar2(800) FORMAT JSON PATH '$.genres')
                            ) j) s
               ON (d.a_id = s.art_id )
               WHEN MATCHED THEN UPDATE 
                SET d.as_of_time = sysdate
               WHEN NOT MATCHED THEN INSERT 
                    (a_id, a_name, follower_cnt, popularity, genres, a_type, as_of_time)
                 VALUES (s.art_id, s.art_name, s.art_follower_cnt, s.art_popularity, s.art_genre, s.art_type, sysdate);
            v_cnt := sql%rowcount;
        end loop;
        commit;
    end get_follows;
    --
    procedure add_follows
    is
        cursor c_main is
            with
                a as
                    (select distinct a_id
                    from track_artists t
                    where pl_owner_name = 'Gary'
                    and pl_name not in ('Comedy',' Audiobooks')
                    and t.a_id not in (select f.a_id from followed_artists f where f.a_id is not null)
                    order by a_id
                    fetch first 40 rows only
                    )
            select listagg('"'||a_id||'"',',') within group (order by a_id) ids
            from a;
        v_res varchar2(2000);
    begin
        commit;
        --
        execute immediate 'ALTER SESSION DISABLE PARALLEL DML';
        execute immediate 'ALTER SESSION DISABLE PARALLEL DDL';
        execute immediate 'ALTER SESSION DISABLE PARALLEL QUERY';
        --
        get_follows;
        --
        insert into track_artists
            (pl_owner_name, pl_name, pl_id, 
            t_id, t_name, t_album_name, 
            a_name, a_id)
        select distinct 
                p.pl_owner_name, p.pl_name, p.pl_id, 
                t.t_id, t.t_name, t.t_album_name,
                j.a_name, j.a_id
        from playlist_tracks t
                join playlists p on p.pl_id = t.pl_id
                outer apply json_table(t.t_artists,'$[*]'
                        columns (
                            a_name        varchar2(400) PATH '$.name' null on empty error on error,
                            a_type        varchar2(400) PATH '$.type' null on empty error on error,
                            a_id          varchar2(400) PATH '$.id' null on empty error on error)
                        ) j
        where t_id not in (select ta.t_id from  track_artists ta);
        --
        commit;
        --
        for r_main in c_main loop
            v_res := fnc_url_to_clob (
                    p_url       =>  'https://api.spotify.com/v1/me/following?type=artist', /*&ids='||r_main.ids,*/
                    p_method    =>  'PUT', 
                    p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                    p_post_data => '{ids:['||r_main.ids||']}'
                    );
            dbms_output.put_line(':'||v_res);
        end loop;
        --
        commit;
        --
    end add_follows;
    --
    procedure set_pl_desc (p_pl_id in varchar2, p_desc in varchar2) 
    is
        v_resp varchar2(4000);
    begin
        v_resp := fnc_url_to_clob (
           p_url       =>  'https://api.spotify.com/v1/playlists/'||p_pl_id,
           p_method    =>  'PUT', 
           p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
           p_post_data => '{"description": "'||p_desc||'"}'
           );
    end set_pl_desc;
    --
end spotify_api;
/
