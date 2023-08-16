--POPULATE TABLES WITH IDS TO DELETE

create table uhs_non_vendor(vendornumber varchar, covid bigint, name_id bigint,  secondaryrepresentative_id bigint,  primaryrepresentative_id bigint,  mailingaddress_id bigint,  designatedaddress_id bigint, covgid bigint, monitorservice_id bigint);

insert into uhs_non_vendor(vendornumber, covid)
values;

update uhs_non_vendor anv set 
name_id = cov.name_id,
secondaryrepresentative_id = cov.secondaryrepresentative_id,
primaryrepresentative_id = cov.primaryrepresentative_id,
mailingaddress_id = cov.mailingaddress_id,
designatedaddress_id = cov.designatedaddress_id,
covgid = cov.vendorgroup_id,
monitorservice_id = cov.monitorservice_id
from clientownedvendor cov
where cov.id = anv.covid;
create table uhs_non_vendor_voc(vocid bigint);

insert into uhs_non_vendor_voc(vocid)
select distinct vendorownercollection_id from clientownedvendorgroup where id in (select covgid from uhs_non_vendor) and vendorownercollection_id is not null;


create table uhs_delete_ms(msid bigint);

insert into uhs_delete_ms(msid)
(select monitorservice_id from uhs_non_vendor
union
select id from monitorservice2 where monitorsubject_id in (select id from monitoredsubject2 where vendorowner_id in (select id from vendorowner where vendorownercollection_id in (select vocid from uhs_non_vendor_voc))));

create table uhs_non_vendor_imrs(imrid bigint);

with recursive imr_down_chain(startingid, id, previousimr_id ) as (
    select id as startingid, id, previousimr_id from intermediatemonitorrecord where id in (select intermediatemonitorrecord_id from monitorrecord where monitorservice_id in (select monitorservice_id from uhs_non_vendor))
    union
    select down.startingid, imr.id, imr.previousimr_id from intermediatemonitorrecord imr inner join imr_down_chain down on down.previousimr_id = imr.id
)
insert into uhs_non_vendor_imrs(imrid)
select distinct id from imr_down_chain where previousimr_id is null;

insert into uhs_non_vendor_imrs(imrid)
select id from intermediatemonitorrecord where monitoritem_id in (select id from monitoritem where monitorservice_id in (select msid from uhs_delete_ms)) and id not in (select imrid from uhs_non_vendor_imrs);

with recursive imr_up_chain as (
    select imrid as startingid, imrid as id, null::bigint as previousimr_id from uhs_non_vendor_imrs
    union
    select up.startingid, imr2.id, imr2.previousimr_id from intermediatemonitorrecord imr2 inner join imr_up_chain up on up.id = imr2.previousimr_id
)
insert into uhs_non_vendor_imrs(imrid)
select id from imr_up_chain where previousimr_id is not null;

create table uhs_non_vendor_sub(subid bigint);

with recursive sub_up_chain as (
    select id, parent_id from monitoredsubject2 where id in (select monitorsubject_id from monitorservice2 where id in (select msid from uhs_delete_ms))
    union
    select sub.id, sub.parent_id from monitoredsubject2 sub inner join sub_up_chain up on sub.id = up.parent_id
)
insert into uhs_non_vendor_sub(subid)
select distinct id from sub_up_chain where parent_id is null;

-- I expect this to insert 0, but I'm doing it because it is theoretically possible
with recursive sub_down_chain(id, parent_id) as (
    select subid, null::bigint as parent_id from uhs_non_vendor_sub
    union
    select sub.id, sub.parent_id from monitoredsubject2 sub inner join sub_down_chain down on sub.parent_id = down.id
)
insert into uhs_non_vendor_sub(subid)
select distinct id from sub_down_chain where parent_id is not null;

insert into uhs_non_vendor_sub(subid)
select distinct id from monitoredsubject2 where vendorowner_id in (select id from vendorowner where vendorownercollection_id in (select vocid from uhs_non_vendor_voc)) and id not in (select subid from uhs_non_vendor_sub);

--vendor connections
create table uhs_non_vendor_vc(vcid bigint);

insert into uhs_non_vendor_vc(vcid)
select id from vendorconnection where vendorgroup_id in (select covgid from uhs_non_vendor);
--IMRs and MRs
delete from monitorrecordnote where monitorrecord_id in (select id from monitorrecord where monitorservice_id in (select msid from uhs_delete_ms));

delete from monitorrecord where monitorservice_id in (select msid from uhs_delete_ms);

delete from intermediatemonitorrecordlabel_assignments where intermediatemonitorrecord_id in (select imrid from uhs_non_vendor_imrs );

delete from intermediatemonitorrecord where id in (select imrid from uhs_non_vendor_imrs order by imrid desc);

--monitor service
update clientownedvendor set monitorservice_id = null where id in (select covid from uhs_non_vendor);

delete from vendorowner_monitorservice2 where monitorservice_id in (select msid from uhs_delete_ms);

delete from monitorservicestatuslog2_users where monitorservicestatuslog2_id in (select id from monitorservicestatuslog2 where service_id in (select msid from uhs_delete_ms));

delete from monitorservicestatuslog2 where service_id in (select msid from uhs_delete_ms);

delete from monitorservicerequest2_monitoritem where monitoritems_id in (select id from  monitoritem where monitorservice_id in (select msid from uhs_delete_ms));

delete from monitoritem where monitorservice_id in (select msid from uhs_delete_ms);

delete from monitorservicerequest2_monitoritem where monitorservicerequest2_id in (select id from  monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms));

delete from fileentity where id in (select response_id from monitorservicerequestlog where servicerequest_id in (select id from monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms)));

delete from filesystementity where id in (select response_id from monitorservicerequestlog where servicerequest_id in (select id from monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms)));

delete from stream where streamid in (select streamid::bigint from fileentity where id in (select response_id from monitorservicerequestlog where servicerequest_id in (select id from monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms))));

delete from monitorservicerequestlog where servicerequest_id in (select id from monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms));

delete from monitorservicerequest2 where monitorservice_id in (select msid from uhs_delete_ms);

delete from monitorservice2 where id in (select msid from uhs_delete_ms);

--MONITOR SUBJECT
delete from monitorservicerequest2_monitoritem where monitoritems_id in (select id from monitoritem where monitorobject_id in (select id from monitorobject where monitorsubject_id in (select subid from uhs_non_vendor_sub)));

delete from monitoritem where monitorobject_id in (select id from monitorobject where monitorsubject_id in (select subid from uhs_non_vendor_sub));

delete from monitorobject where monitorsubject_id in (select subid from uhs_non_vendor_sub);

delete from monitoredsubject2 where id in (select subid from uhs_non_vendor_sub);
--COV
delete from monitoredsubject2 where vendor_id in (select covid from uhs_non_vendor);

delete from clientownedvendorevent where clientownedvendor_id in (select covid from uhs_non_vendor);

delete from enrollmentmetadata where clientownedvendor_id in (select covid from uhs_non_vendor);

delete from clientownedvendorlabel_assignments where clientownedvendor_id in (select covid from uhs_non_vendor);

delete from clientownedvendor_clientvendorconnection where clientownedvendor_id in (select covid from uhs_non_vendor);

delete from lawsonvendor where clientownedvendor_id in (select covid from uhs_non_vendor);

delete from clientownedvendor cov where id in (select covid from uhs_non_vendor);

delete from address_line where id in (select mailingaddress_id from uhs_non_vendor);

delete from address_region where id in (select mailingaddress_id from uhs_non_vendor);

delete from address where id in (select mailingaddress_id from uhs_non_vendor);

delete from address_line where id in (select designatedaddress_id from uhs_non_vendor);

delete from address_region where id in (select designatedaddress_id from uhs_non_vendor);

delete from address where id in (select designatedaddress_id from uhs_non_vendor);

--COVG And VO
update vendorconnection set vendorgroup_id = null where id in (select vcid from uhs_non_vendor_vc);

delete from monitoredsubject2 where vendorgroup_id in (select covgid from uhs_non_vendor);

delete from vendorgroupenrollment where vendorgroup_id in (select covgid from uhs_non_vendor);

delete from clientownedvendorgroup_w9 where clientownedvendorgroup_id in (select covgid from uhs_non_vendor);

delete from clientownedvendorgroup where id in (select covgid from uhs_non_vendor);

delete from monitoredsubject2 where vendorowner_id in (select id from vendorowner where vendorownercollection_id in (select vocid from uhs_non_vendor_voc));

delete from vendorowner_monitorservice2 where vendorowner_id in (select id from vendorowner where vendorownercollection_id in (select vocid from uhs_non_vendor_voc));

--These are not needed as the vendorowners are associated with actual vendors and contain no PII (see 2023 parts 1 and 2)
--delete from vendorowner where vendorownercollection_id in (select vocid from uhs_non_vendor_voc);

--update vendor set vendorownercollection_id = null where vendorownercollection_id in (select vocid from uhs_non_vendor_voc);

--delete from vendorownercollection where id in (select vocid from uhs_non_vendor_voc);
--These are super slow due to a gajillion foreign keys. If there was any data in them we could delete them but there is no personal data in them

--delete from role where id in (select primaryrepresentative_id from uhs_non_vendor);

--delete from role where id in (select secondaryrepresentative_id from uhs_non_vendor);
-- I am not going to put deleting of vendors in here because we really want to think hard about that before we do it in the future. If they have any other vendor connections, we shouldn't delete them. We don't want to delete them anyway, but in this case, we have to!