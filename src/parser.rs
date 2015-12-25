use nom::{alphanumeric,space};
use std;

pub enum Command {
    Exit,
    Help,
    Load( String ),
    Query( Vec<String>, Vec<Filter> ),
}

pub struct Filter {
    pub field_name: String,
    pub filter_type: String,
    pub params: Vec<String>,
    pub value: String,
}

named!(
    pub cmd<Command>,
    alt!(
        exit
      | help
      | load
      | query
    )
);

named!(
    pub exit<Command>,
    chain!(
        tag!("EXIT"),
        || { Command::Exit }
    )
);

named!(
    pub help<Command>,
    chain!(
        tag!("HELP"),
        || { Command::Help }
    )
);

named!(
    pub field_names<Vec<String> >,
    alt!(
        tag!("*") => { |_| Vec::new() }
        | chain! (
            field_name: id ~
            field_names: many0!(
                chain!(
                    opt!(space) ~
                    tag!(",") ~
                    opt!(space) ~
                    field_name: id,
                    || field_name
                )
            ),
            || {
                let mut rtn_field_names = vec!(field_name);
                for field_name in field_names {
                    rtn_field_names.push(field_name);
                }

                rtn_field_names
            }
        )
    )
);

named!(
    pub filename<String>,
    chain!(
        chars: many1!(
            map_res!(
                alt!(
                    tag!("-") | tag!("_") | tag!("/") | tag!(".") | alphanumeric
                ), 
                std::str::from_utf8
            )
        ),
        || {
            chars.into_iter().fold(
                "".to_string(), 
                |mut f, c| {
                   f.push_str(c);
                   f
                }
            )
        }
    )
);

named!(
    pub filter<Filter>,
    chain!(
        field_name: id ~
        space ~
        tag!("~") ~
        filter_type: unquoted_id ~
        params : filter_params ~
        space ~
        value: id,
        || Filter { field_name: field_name, filter_type: filter_type, params: params,  value: value }
    )
);

named!(
    pub id<String>,
    alt!(quoted_id | unquoted_id)
);

named!(
    pub load<Command>,
    chain!(
        tag!("LOAD") ~
        space ~
        f: filename,
        || { Command::Load(f) }
    )
);

named!(
    pub filter_params<Vec<String> >,
    chain!(
        tag!("(") ~
        params: opt_res!(
            chain! (
                param: id ~
                params: many0!(
                    chain!(
                        opt!(space) ~
                        tag!(",") ~
                        opt!(space) ~
                        param: id,
                        || param
                    )
                ),
                || {
                    let mut rtn_params = vec!(param);
                    for param in params {
                        rtn_params.push(param);
                    }

                    rtn_params
                }
            )
        ) ~
        tag!(")"),
        || { 
            match params {
                Ok(_) => params.unwrap(),
                Err(_) => vec!(),
            }
        }
    )
);

named!(
    pub query<Command>,
    chain!(
        tag!("SELECT") ~
        space ~
        field_names: field_names ~
        space ~
        tag!("WHERE") ~
        space ~
        f: filter ~
        filters: many0!(
            chain!(
                space ~
                tag!("AND") ~
                space ~
                f: filter,
                || { f }
            )
        ),
        || {
            let mut rtn_filters = vec!(f);
            for filter in filters {
                rtn_filters.push(filter);
            }

            Command::Query(
                field_names,
                rtn_filters,
            )
        }
    )
);

named!(
    pub quoted_id<String>,
    chain!(
        tag!("\"") ~
        chars: many1!(
            map_res!(
                alt!(
                    tag!("-") | tag!("_") | tag!(".") | tag!(" ") | tag!("\\\"") | alphanumeric
                ),
                std::str::from_utf8
            )
        ) ~
        tag!("\""),
        || {
            chars.into_iter().fold(
                "".to_string(), 
                |mut f, c| {
                   f.push_str(c);
                   f
                }
            )
        }
    )
);

named!(
    pub unquoted_id<String>,
    chain!(
        chars: many1!(
            map_res!(
                alt!(
                    tag!("-") | tag!("_") | tag!(".") | alphanumeric
                ), 
                std::str::from_utf8
            )
        ),
        || {
            chars.into_iter().fold(
                "".to_string(), 
                |mut f, c| {
                   f.push_str(c);
                   f
                }
            )
        }
    )
);
