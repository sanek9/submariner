/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeproxy_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/submariner-io/submariner/pkg/routeagent_driver/handlers/kubeproxy"
)

var _ = Describe("ParseRouteTables", func() {
	DescribeTable("valid input",
		func(value string, expected []int) {
			tables, err := kubeproxy.ParseRouteTables(value)
			Expect(err).NotTo(HaveOccurred())
			Expect(tables).To(Equal(expected))
		},
		Entry("empty", "", nil),
		Entry("whitespace", "  ", nil),
		Entry("single table", "2", []int{2}),
		Entry("multiple tables", "2,3,4", []int{2, 3, 4}),
		Entry("spaces around values", " 2, 3 ,4 ", []int{2, 3, 4}),
		Entry("deduplicates", "2,3,2", []int{2, 3}),
		Entry("skips reserved main/local/default", "2,254,255,253,3", []int{2, 3}),
		Entry("skips Submariner tables", "2,149,150,3", []int{2, 3}),
	)

	DescribeTable("invalid input",
		func(value string) {
			_, err := kubeproxy.ParseRouteTables(value)
			Expect(err).To(HaveOccurred())
		},
		Entry("non-numeric", "2,abc"),
		Entry("zero", "0"),
		Entry("negative", "-1"),
	)
})
