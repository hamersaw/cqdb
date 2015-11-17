use nom::{alpha,alphanumeric,space};
use std;

pub enum Command {
    Query( Vec<String>, Vec<Filter> ),
    Load( String ),
    Help,
}

named!(
    pub cmd<Command>,
    alt!(
        query
      | load
      | help
    )
);

named!(
    pub load<Command>,
    chain!(
        tag!("LOAD") ~
        space ~
        file_name: id,
        || { Command::Load(file_name) }
    )
);

named!(
    pub help<Command>,
    chain!(
        tag!("HELP"),
        || { Command::Help }
    )
);

#[derive(Clone)]
pub struct Filter {
    pub field_name: String,
    pub filter_type: String,
    pub value: String,
}


named!(
    pub filter<Filter>,
    chain!(
        field_name: id ~
        space ~
        tag!("~") ~
        filter_type: id ~
        space ~
        value: id,
        || Filter { field_name: field_name, filter_type: filter_type, value: value }
    )
);

named!(
    pub field_names<Vec<String> >,
    alt!(
        tag!("*") => { |_| Vec::new() }
        | chain! (
            field_name: alpha ~
            field_names: many0!(
                chain!(
                    opt!(space) ~
                    tag!(",") ~
                    opt!(space) ~
                    field_name: alpha,
                    || field_name
                )
            ),
            || {
                let mut rtn_field_names = vec!(std::str::from_utf8(field_name).unwrap().to_string());
                for field_name in field_names {
                    rtn_field_names.push(std::str::from_utf8(field_name).unwrap().to_string());
                }

                rtn_field_names
            }
        )
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
                and_filter: filter,
                || { and_filter }
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

// Matches a setting name of the form [-a-zA-Z0-9_/.]+
named!(
    id<&[u8], String>,
    chain!(
        t: many1!(
            map_res!(
                alt!(
                    tag!("-")
                  | tag!("_")
                  | tag!("/")
                  | tag!(".")
                  | alphanumeric
                ), 
                std::str::from_utf8
            )
        ),
        || {
            t.into_iter().fold(
                "".to_string(), 
                |mut accum, slice| {
                   accum.push_str(slice);
                   accum
                }
            )
        }
    )
);
