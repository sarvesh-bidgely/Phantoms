import enum

class GenerateEnrolementValues():
    enrolement_file_values = []
    enrolement_file_values.append(buildCustomerId())
    enrolement_file_values.append(buildAccountId())


    def buildCustomerId(self):
        self.customerId = "12344"
        return self.customerId

    def buildAccountId(self):
        self.accountId = "12344"
        return self.accountId

    def buildPremiseId(self):
        self.premiseId = "12344"
        return self.premiseId




