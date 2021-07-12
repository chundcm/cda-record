from snmp_model_discovery import *


@Supported('DELL_IDRAC')
class Dell_DIRAC(ModelDiscover):
    def discoverSerialNumber(self):
        self.add_dell_server_model()
        self.add_dell_idrac_model()
        self.add_dell_cpu_model()
        self.add_dell_interface_model()
        self.add_dell_disk_model()


    def add_dell_server_model(self):
        model = self.model
        oid_system_fqdn = '1.3.6.1.4.1.674.10892.5.1.3.1.0'
        oid_system_model = '1.3.6.1.4.1.674.10892.5.1.3.12.0'
        oid_system_service_tag = '1.3.6.1.4.1.674.10892.5.1.3.2.0'

        k = self.snmpGet(oid_system_fqdn, oid_system_model, oid_system_service_tag)
        model.DnsName = k[0][1]
        model.Model = k[1][1]
        if k[2][1]:
            model.SerialNumber.Chassis[0].SerialNumber = k[2][1]
        else:
            model.SerialNumber.Chassis[0].SerialNumber = NOT_FOUND_IN_MIB

    def add_dell_idrac_model(self, node_osh=None):
        model = self.model
        oid_name = '1.3.6.1.4.1.674.10892.5.1.1.1.0'
        oid_short_name = '1.3.6.1.4.1.674.10892.5.1.1.2.0'
        oid_description = '1.3.6.1.4.1.674.10892.5.1.1.3.0'
        oid_vendor = '1.3.6.1.4.1.674.10892.5.1.1.4.0'
        oid_version = '1.3.6.1.4.1.674.10892.5.1.1.5.0'

        midx = sizeof(model.Method) if model.Method else 0
        aidx = 0
        k = self.snmpGet(oid_name, oid_short_name, oid_description, oid_vendor, oid_version)
        model.Method[midx].Attribute[aidx].Name = k[0][1]
        model.Method[midx].Attribute[aidx].ShortName = k[1][1]
        model.Method[midx].Attribute[aidx].Description = k[2][1]
        model.Method[midx].Attribute[aidx].Vendor = k[3][1]
        model.Method[midx].Attribute[aidx].Version = k[4][1]
        if node_osh:
            model.Method[midx].Attribute[aidx].Containment = node_osh
        self.add_resource_to_model(model.Method[midx].Attribute[aidx], 'remote_management_card')


    def add_dell_cpu_model(self):
        model = self.model
        midx = sizeof(model.Method) if model.Method else 0
        device_types = self.snmpWalk('1.3.6.1.4.1.674.10892.5.4.1100.30.1.7')
        if device_types:
            aidx = 0
            for j, (device_type_oid, device_type) in enumerate(device_types):
                if device_type:
                    index = OID(device_type_oid).serials()[15]
                    model.Method[midx].Attribute[aidx].PrinterCpuId = 'CPU' + str(aidx)
                    name = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.4.1100.30.1.23.1.' + index)
                    if name:
                        model.Method[midx].Attribute[aidx].Name = name
                    self.add_resource_to_model(model.Method[midx].Attribute[aidx], 'cpu')
                    aidx += 1


    def add_dell_interface_model(self):
        model = self.model
        midx = sizeof(model.Method) if model.Method else 0
        interface_names = self.snmpWalk('1.3.6.1.4.1.674.10892.5.4.1100.90.1.6')
        if interface_names:
            aidx = 0
            for j, (interface_name_oid, interface_name) in enumerate(interface_names):
                if interface_name:
                    index = OID(interface_name_oid).serials()[15]
                    model.Method[midx].Attribute[aidx].Name = interface_name
                    mac = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.4.1100.90.1.16.1.' + index)
                    if mac:
                        model.Method[midx].Attribute[aidx].MacAddr = mac
                    self.add_resource_to_model(model.Method[midx].Attribute[aidx], 'interface')


    def add_dell_disk_model(self):
        model = self.model
        midx = sizeof(model.Method) if model.Method else 0
        disk_names = self.snmpWalk('1.3.6.1.4.1.674.10892.5.5.1.20.130.4.1.2')
        if disk_names:
            aidx = 0
            for j, (disk_name_oid, disk_name) in enumerate(disk_names):
                if disk_name:
                    index = OID(disk_name_oid).serials()[15]
                    model.Method[midx].Attribute[aidx].Name = disk_name
                    serial_number = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.5.1.20.130.4.1.7.' + index)
                    if serial_number:
                        model.Method[midx].Attribute[aidx].SerialNumber = serial_number
                    disk_size = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.5.1.20.130.4.1.11.' + index)
                    if disk_size:
                        model.Method[midx].Attribute[aidx].Size = disk_size
                    manufacture = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.5.1.20.130.4.1.3.' + index)
                    if manufacture:
                        model.Method[midx].Attribute[aidx].Vendor = manufacture
                    product_id = self.snmpGetValue('1.3.6.1.4.1.674.10892.5.5.1.20.130.4.1.6.' + index)
                    if product_id:
                        model.Method[midx].Attribute[aidx].Model = product_id
                    self.add_resource_to_model(model.Method[midx].Attribute[aidx], 'disk_device')
                    aidx += 1